"""
Раздел для выставления заключения загруженным образцам
"""
import json

import requests
import pandas as pd

from . import common


CONCLUSION_PIPE_SETTINGS = common.load_config(f"{common.WORKING_PATH}/conclusion_pipe_settings.yaml")


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
        vga_request = requests.get(common.BASE_URL + CONCLUSION_PIPE_SETTINGS["paths"]["conclusion_types"],
                                   headers=common.default_settings["access"]["headers"])
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


def read_and_prepare_data(df: pd.DataFrame, pango_path: str, clades_path: str) -> dict:
    """
    Функция для прочтения входных данных и их подготовки. Под входными данными подразумевается таблица вида FULL_TABLE,
    файл с результатами работы Pangolin по этим образцам и файл с результатами работы NextClade по этим образцам.
    Проходит проверка соответствия имен образцов и дополнение первичной таблицы данных результатами работы
    сторонних программ. \n \n
    :param df: уже прочитанный DataFrame с данными после второго этапа;
    :param pango_path: путь к текстовой таблице с результатами работы Pangolin;
    :param clades_path: путь к текстовому json-файлу с результатами работы NextClade;
    :return: STATE-словарь, payload - DataFrame с обновленной информацией образцов в случае успеха
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        pango = pd.read_csv(pango_path)  # тут не добавляем разделитель, так как панголин всегда сохраняет адекватно
        with open(clades_path, "r") as file_read:
            clades = json.load(file_read)['results']  # тут сразу берем лишь тот кусок, с которым удобно работать
        # итерируемся по результатам Pango и добавляем результаты в нашу таблицу сведением
        for idx, row in pango.iterrows():
            if row['taxon'] in df.index:  # чтобы не набрать лишнего в таблицу
                df.loc[row['taxon'], 'pango'] = row['lineage']
        cur_counter = df[(df['valid_seq']) & (df['pango'] == "")].shape[0]
        if cur_counter != 0:
            raise AssertionError(f"Как минимум один ({cur_counter}) из валидных образцов не получил результата Pango")

        # теперь проставим результаты Clades
        for row in clades:
            barcode = row['seqName'].replace(" ", "_")
            if barcode in df.index:  # чтобы не набрать лишнего в таблицу
                df.loc[barcode, 'nextclade'] = row['clade']
        cur_counter = df[(df['valid_seq']) & (df['nextclade'] == "")].shape[0]
        if cur_counter != 0:
            raise AssertionError(f"Как минимум один ({cur_counter}) из валидных образцов не получил результата Clades")

    except Exception as e:
        response['payload'] = str(e)
    else:
        # если все ок, то возвращаем обновленную табличку и хороший статус
        response['success'] = True
        response['payload'] = df

    return response


# TODO: обновить принцип выставления локального заключения
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
        df['sequence_conclusion_local'] = df.apply(
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


def request_samples_info(rdf: pd.DataFrame, increment: int = 40) -> dict:
    """
    Функция для запроса информации об образцах на основе их имен.
    Необходимо учитывать, что сервер не предоставляет информации больше,
    чем для 50 образцов, ввиду ограничений размеров таблицы, поэтому неизбежен цикличный запрос информации.
    По этой же причине придется сразу после запроса информации проводить парсинг результатов и обновление таблицы,
    так как нужно где-то хранить промежуточные результатами между запросами по <50 образцов.
    Все локальные ошибки единичных образцов, т.е. не критических, будут записаны в поле 'status' таблицы.
    Критические же ошибки (например, `500 Server Error`) должны останавливать работу всей функции.  \n \n
    :param increment:
    :param rdf: таблица с данными образцов;
    :return: STATE-словарь, payload - DataFrame с обновленными данными в случае успеха.
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        df = rdf[rdf['sample_status_remote'] == 'Uploaded']
        names_array = df['sample_number'].values
        idx = 0
        while idx < names_array.size:
            concatenated_names = ", ".join(names_array[idx:idx+increment])  # получаем строку запроса
            samples_info = requests.get(common.BASE_URL + CONCLUSION_PIPE_SETTINGS["paths"]["samples_info"],
                                        headers=common.default_settings["access"]["headers"],
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
                        rdf.loc[ali_index, 'sequence_vga_id'] = row['id']
                    else:
                        raise AssertionError(f"Найдено {len(ali_index)} соответствий для " +
                                             f"`{row['sample']['sample_number']}` (id {row['id']})")
            else:
                raise AssertionError(f"Request for {idx}:{idx + increment} failed with {samples_info.status_code}")
            idx += increment
        df = rdf[rdf['sample_status_remote'] == 'Uploaded']
        cur_counter = df[df['sequence_vga_id'] == ""].shape[0]
        if cur_counter != 0:
            raise AssertionError(f"Как минимум один ({cur_counter}) из образцов не получил id с портала")

    except Exception as e:
        response['payload'] = str(e)
    else:
        # если все ок, то возвращаем обновленную табличку и хороший статус
        response['success'] = True
        response['payload'] = rdf

    return response


def state_conclusion_remote(rdf: pd.DataFrame, increment: int = 40) -> dict:
    """
    Функция для отправки результатов заключений на сервер. Аналогично запросу образцов, необходимо поэтапное (по 50
    образцов) выставление результатов. \n \n
    :return:
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        # сперва выделяем группы образцов
        df = rdf[rdf['sample_status_remote'] == 'Uploaded']
        unique_results = df['sequence_conclusion_local'].unique()
        for result_var in unique_results:
            sub_df = df[df['sequence_conclusion_local'] == result_var]
            if result_var != "NS":
                # делаем выборку DataFrame
                ids_list = sub_df['sequence_vga_id'].to_list()
                # итерационно перебираем для выставления результата
                idx = 0
                while idx < len(ids_list):
                    change_req = requests.post(common.BASE_URL + CONCLUSION_PIPE_SETTINGS["paths"]["state_res"],
                                               headers=common.default_settings["access"]["headers"],
                                               data=json.dumps(
                                                   {
                                                       "uploads": ids_list[idx:idx+increment],
                                                       "result_type": CONCLUSION_PIPE_SETTINGS["conclusions"]["vga_conclusion_types"][result_var],
                                                       "comment": "Auto results"
                                                   }
                                               ))
                    if change_req.status_code == 200:
                        rdf.loc[sub_df.index, "sequence_conclusion_remote"] = "OK"
                        idx += increment
                    else:
                        raise AssertionError(f"Request for {idx}:{idx + increment} failed with {change_req.status_code}")
            else:
                rdf.loc[sub_df.index, "sequence_conclusion_remote"] = "Unknown conclusion"
    except Exception as e:
        response['payload'] = str(e)
    else:
        # если все ок, то возвращаем обновленную табличку и хороший статус
        response['success'] = True
        response['payload'] = rdf

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
