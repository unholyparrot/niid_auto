"""
Раздел для выставления заключения загруженным образцам
"""
import json

import requests
import yaml
import pandas as pd

from . import common


with open(f"{common.WORKING_PATH}/conclusion_pipe_settings.yaml", "r", encoding="utf-8") as fr:
    CONCLUSION_PIPE_SETTINGS = yaml.load(fr, Loader=yaml.SafeLoader)

BASE_URL = CONCLUSION_PIPE_SETTINGS["paths"]["base"]


def state_token(token: str) -> dict:
    """
    Функция для внесения токена авторизации и дальнейшего доступа на портал.
    Запрашивает информацию о пользователе в текущей сессии, если это не удается, значит токен введен некорректно. \n \n
    :param token: токен авторизации вида `58bac2b5851a4a7a832c30a02271045ef7b476e599134a19bc159e3ff7468e31`;
    :return: словарь вида STATE, payload - информация об авторизированном пользователе в случае успеха.
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        CONCLUSION_PIPE_SETTINGS["access"]["token"] = token
        CONCLUSION_PIPE_SETTINGS["access"]["headers"]["Authorization"] = f"Bearer {token}"
        test_request = requests.get(BASE_URL + CONCLUSION_PIPE_SETTINGS["paths"]["ping"],
                                    headers=CONCLUSION_PIPE_SETTINGS["access"]["headers"])
    except Exception as e:
        response['payload'] = str(e)
    else:
        if test_request.status_code == 200:
            response['success'] = True
            response['payload'] = test_request.json()
        else:
            # здесь можно было бы сделать аналогично возврат через json, но ошибочный запрос
            # не всегда может иметь json-представление, что вызовет ошибку после Try-Catch конструкции
            response['payload'] = f"Токен не подходит: {test_request.text}"

    return response


def request_possible_conclusions():
    """
    Функция для запроса всех возможных заключений с портала, проверяет соответствие сохраненных локально вариантов
    свежим вариантам.
    Если варианты с портала хоть как-то не совпадают с вариантами, представленными в базе данных,
    то 'success': False, так как необходимо устанавливать соответствия вручную. \n \n
    :return: STATE-словарь, paylaod - текущий словарь и\или сообщение об ошибке, success как индикатор успеха
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        vga_request = requests.get(BASE_URL + CONCLUSION_PIPE_SETTINGS["paths"]["conclusion_types"],
                                   headers=CONCLUSION_PIPE_SETTINGS["access"]["headers"])
    except Exception as e:
        response['payload'] = str(e)
    else:
        if vga_request.status_code == 200:
            comparison_dict = dict()
            for elem in vga_request.json():
                comparison_dict[elem['text']] = elem['value']
            if comparison_dict == CONCLUSION_PIPE_SETTINGS["conclusions"]["vga_conclusion_types"]:
                response['success'] = True
                response['payload'] = comparison_dict
            else:
                # здесь будет возвращаться False и новый словарь, в соответствии с которым нужно провести
                # обновление вариантов заключений
                response['payload'] = comparison_dict
        else:
            response['payload'] = f"{vga_request.status_code}: {vga_request.text}`"

    return response


def read_and_prepare_data(table_path: str, pango_path: str, clades_path: str,
                          table_separator: str = "\t") -> dict:
    """
    Функция для прочтения входных данных и их подготовки. Под входными данными подразумевается таблица вида FULL_TABLE,
    файл с результатами работы Pangolin по этим образцам и файл с результатами работы NextClade по этим образцам.
    Проходит проверка соответствия имен образцов и дополнение первичной таблицы данных результатами работы
    сторонних программ. \n \n
    :param table_path: путь к текстовой таблице с информацией об образцах;
    :param pango_path: путь к текстовой таблице с результатами работы Pangolin;
    :param clades_path: путь к текстовому json-файлу с результатами работы NextClade;
    :param table_separator: опциональный разделитель для таблицы образцов;
    :return: STATE-словарь, payload - DataFrame с обновленной информацией образцов в случае успеха
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        df = pd.read_csv(table_path, sep=table_separator)  # считываем таблицу с данными
        # TODO: добавить проверку столбцов или прикрутить мануальное введение наименований
        pango = pd.read_csv(pango_path)  # тут не добавляем разделитель, так как панголин всегда сохраняет адекватно
        with open(clades_path, "r") as file_read:
            clades = json.load(file_read)['results']  # тут сразу берем лишь тот кусок, с которым удобно работать
        # добавляем нужные для работы столбцы таблице
        for heading in CONCLUSION_PIPE_SETTINGS["table"]:
            df[heading] = ""

        # итерируемся по результатам Pango и добавляем результаты в нашу таблицу сведением
        for idx, row in pango.iterrows():
            ali_index = df[df['barcode'] == row['taxon']].index
            # по-хорошему надо поднимать ошибку, когда есть лишнее имя, но пока это спорно для реализации
            if len(ali_index) == 1:
                df.loc[ali_index[0], 'pango'] = row['lineage']
            # else:
            #     raise AssertionError(f"Found {len(ali_index)} matches for `{row['taxon']}`")
            # устроим проверку иначе, будем искать незаполненные столбцы
        cur_counter = df[df['pango'] == ""].shape[0]
        if cur_counter != 0:
            raise AssertionError(f"Как минимум один ({cur_counter}) из образцов не получил результата Pango")

        # теперь проставим результаты Clades
        for row in clades:
            ali_index = df[df['barcode'] == row['seqName'].replace(" ", "_")].index
            if len(ali_index) == 1:
                df.loc[ali_index[0], 'nextclade'] = row['clade']
            # else:
            #     raise AssertionError(f"Found {len(ali_index)} matches for `{row['seqName']}`")
            # устроим проверку иначе, будем искать незаполненные столбцы
        cur_counter = df[df['nextclade'] == ""].shape[0]
        if cur_counter != 0:
            raise AssertionError(f"Как минимум один ({cur_counter}) из образцов не получил результата Clades")

    except Exception as e:
        response['payload'] = str(e)
    else:
        # если все ок, то возвращаем обновленную табличку и хороший статус
        response['success'] = True
        response['payload'] = df

    return response


def state_conclusion_local(df: pd.DataFrame) -> dict:
    """
    Функция для определения заключения по сводным результатам Pangolin и NextClade. Не отправляет запроса на сервер,
    но локально подтягивает заключение и его соответствие возможным вариантам на сервере. \n \n
    :param df: таблица с данными образцов;
    :return: STATE-словарь, payload - DataFrame с обновленными данными в случае успеха.
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        # считываем словарь соответствий локальных заключений и заключений VGARus
        cl_dict = CONCLUSION_PIPE_SETTINGS["conclusions"]["local"]
        # локально проставляем заключения в соответствии с настройками
        df['result'] = df.apply(
            lambda x: cl_dict[x.pango + "|" + x.nextclade] if cl_dict.get(x.pango + "|" + x.nextclade) else "NS",
            axis=1
        )
    except Exception as e:
        response['payload'] = str(e)
    else:
        # если все ок, то возвращаем обновленную табличку и хороший статус
        response['success'] = True
        response['payload'] = df

    return response


def request_samples_info(df: pd.DataFrame, increment: int = 40) -> dict:
    """
    Функция для запроса информации об образцах на основе их имен.
    Необходимо учитывать, что сервер не предоставляет информации больше,
    чем для 50 образцов, ввиду ограничений размеров таблицы, поэтому неизбежен цикличный запрос информации.
    По этой же причине придется сразу после запроса информации проводить парсинг результатов и обновление таблицы,
    так как нужно где-то хранить промежуточные результатами между запросами по <50 образцов.
    Все локальные ошибки единичных образцов, т.е. не критических, будут записаны в поле 'status' таблицы.
    Критические же ошибки (например, `500 Server Error`) должны останавливать работу всей функции.  \n \n
    :param increment:
    :param df: таблица с данными образцов;
    :return: STATE-словарь, payload - DataFrame с обновленными данными в случае успеха.
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        names_array = df['sample_number'].values
        idx = 0
        while idx < names_array.size:
            concatenated_names = ", ".join(names_array[idx:idx+increment])  # получаем строку запроса
            samples_info = requests.get(BASE_URL + CONCLUSION_PIPE_SETTINGS["paths"]["samples_info"],
                                        headers=CONCLUSION_PIPE_SETTINGS["access"]["headers"],
                                        params={
                                            "filter": json.dumps(
                                                {
                                                    "sample_number": concatenated_names
                                                }
                                            )
                                        }
                                        )
            if samples_info.status_code == 200:
                for row in samples_info.json():
                    ali_index = df[df['sample_number'] == row['sample']['sample_number']].index
                    if len(ali_index) == 1:
                        df.loc[ali_index[0], 'true_id'] = row['id']
                    else:
                        raise AssertionError(f"Найдено {len(ali_index)} соответствий для " +
                                             f"`{row['sample']['sample_number']}` (id {row['id']})")
            else:
                raise AssertionError(f"Request for {idx}:{idx + increment} failed with {samples_info.status_code}")
            idx += increment
        cur_counter = df[df['true_id'] == ""].shape[0]
        if cur_counter != 0:
            raise AssertionError(f"Как минимум один ({cur_counter}) из образцов не получил id с портала")

    except Exception as e:
        response['payload'] = str(e)
    else:
        # если все ок, то возвращаем обновленную табличку и хороший статус
        response['success'] = True
        response['payload'] = df

    return response


def state_conclusion_remote(df: pd.DataFrame, increment: int = 40) -> dict:
    """
    Функция для отправки результатов заключений на сервер. Аналогично запросу образцов, необходимо поэтапное (по 50
    образцов) выставление результатов. \n \n
    :return:
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        # сперва выделяем группы образцов
        unique_results = df['result'].unique()
        for result_var in unique_results:
            sub_df = df[df['result'] == result_var]
            if result_var != "NS":
                # делаем выборку DataFrame
                ids_list = sub_df['true_id'].to_list()
                # итерационно перебираем для выставления результата
                idx = 0
                while idx < len(ids_list):
                    change_req = requests.post(BASE_URL + CONCLUSION_PIPE_SETTINGS["paths"]["state_res"],
                                               headers=CONCLUSION_PIPE_SETTINGS["access"]["headers"],
                                               data=json.dumps(
                                                   {
                                                       "uploads": ids_list[idx:idx+increment],
                                                       "result_type": CONCLUSION_PIPE_SETTINGS["conclusions"]["vga_conclusion_types"][result_var],
                                                       "comment": "Auto results"
                                                   }
                                               ))
                    if change_req.status_code == 200:
                        df.loc[sub_df.index, "status"] = "OK"
                        idx += increment
                    else:
                        raise AssertionError(f"Request for {idx}:{idx + increment} failed with {change_req.status_code}")
            else:
                df.loc[sub_df.index, "status"] = "Unknown conclusion"
    except Exception as e:
        response['payload'] = str(e)
    else:
        # если все ок, то возвращаем обновленную табличку и хороший статус
        response['success'] = True
        response['payload'] = df

    return response


# TODO: добавить проверку выставленного заключения
def check_conclusion_success():
    """
    Функция для проверки успеха проставления заключений на сайте. В случае полного соответствия проставляет 'OK' в поле
    'status' итоговой таблицы. Производит поэтапные запросы (по 50 образцов). 
    :return: 
    """
    # TBD
    pass
