"""
Раздел, отвечающий за составление сборной таблицы образцов по входным таблицам Литеха
и внутренним таблица из Google Sheets.
Представленному разделу соответствует `registry_pipe_settings.yaml` с некоторыми константами, а также
ноутбук registry_pipe_example.ipynb в качестве образца работы.
NB: все функции, производящие манипуляции с DataFrame, делают их inplace, то есть возвращаются не копии.
"""
import concurrent.futures
import re

import pandas as pd
import requests
import transliterate

from . import common


REGISTRY_PIPE_SETTINGS = common.load_config(f"{common.WORKING_PATH}/registry_pipe_settings.yaml")


def read_input_tables(table_2_path: str, table_3_path: str, separator='\t') -> dict:
    """
    Функция для загрузки в память входных таблиц, с которыми ведется работа.
    Предполагается, что все таблицы, что будут поданы на вход, не имеют наименований столбцов.
    Проверяется, найдено ли минимально допустимое (размер плашки) пересечение по количеству образцов между
    таблицами 2 и 3. \n \n
    :param table_2_path: путь к таблице, содержащей данные из Таблицы 2 (НИИД, Google Sheets);
    :param table_3_path: путь к таблице, содержащей данные из Таблицы 3 (Литех, Google Sheets);
    :param separator: разделитель данных в текстовых файлах;
    :return: словарь вида STATE, payload - DataFrame с пересечением таблиц в случае успеха
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        df2 = pd.read_csv(table_2_path,
                          sep=separator, dtype=str, encoding="utf-8",
                          names=REGISTRY_PIPE_SETTINGS["column_names"]["from_2"])
        if any(df2["Sample_name"].duplicated()):
            raise AssertionError(f"Дублирующиеся 'Sample_name': " +
                                 f"{', '.join(df2[df2['Sample_name'].duplicated()]['Sample_name'].tolist())}")
        # создаем баркоды
        barcodes = df2["Dispence_to"].apply(lambda x: f"{x}" if len(x) > 1 else f"0{x}")
        # и словарь соответствий баркода штрихкоду Литеха
        barcodes_dict = {x: y for x, y in zip(df2["Sample_name"], barcodes)}
        df3 = pd.read_csv(table_3_path,
                          sep=separator, dtype=str, encoding="utf-8",
                          names=REGISTRY_PIPE_SETTINGS["column_names"]["from_3"])
        if any(df3["litech_barcode"].duplicated()):
            raise AssertionError(f"Дублирующиеся 'litech_barcode': " +
                                 f"{', '.join(df3[df3['litech_barcode'].duplicated()]['litech_barcode'].tolist())}")
        df_res = df3[df3["litech_barcode"].isin(df2["Sample_name"])].copy(deep=True)
    except Exception as e:
        response['payload'] = str(e)
    else:
        # проверяем, соответствует ли число образцов в таблице пересечения числу образцов в плашке
        if df_res.shape[0] == df2.shape[0]:
            # передаем созданные баркоды выбранным образцам
            try:
                df_res["barcode"] = df_res["litech_barcode"].apply(lambda x: f"barcode{barcodes_dict[x]}_MN908947.3")
            # если хоть какой-то образец не получит баркод, возникнет KeyError
            except KeyError as k_err:
                response['payload'] = f"Какому-то образцу не удалось выставить баркод: {str(k_err)}"
            # если все прошло без ошибок, то передаём "красивую" таблицу на выход
            else:
                response['success'] = True
                response['payload'] = df_res
        # уведомляем пользователя о несовпадении числа образцов
        else:
            response['payload'] = f"Не совпадает число образцов в плашке и число найденных " \
                                  f"образцов в таблице образцов: {df_res.shape[0]} != {df2.shape[0]}"

    return response


def single_registry_request(registry_id):
    """
    Производит запрос информации по 'registry_id' реестру. Функция может использоваться как циклично, так и для
    конкурентных запросов. \n \n
    :param registry_id: ID реестра для запроса.
    :return: request в сыром виде
    """
    lil_request = requests.get(common.BASE_URL + REGISTRY_PIPE_SETTINGS["paths"]["registry_query"] + str(registry_id),
                               headers=common.default_settings["access"]["headers"])
    return lil_request


def update_registry_info(path_registry_table: str) -> dict:
    """
    Функция для запроса таблицы соответствия образцов реестрам. Использует конкурентные запросы. Для успешной работы
    необходимо предварительное объявление токена через раздел common. Работает медленно, но все же быстрее, чем обычный
    запрос через цикл. \n \n
    :param path_registry_table: путь для сохранения таблицы реестров.
    :return: словарь вида STATE, payload - DataFrame соответствия образцов реестрам
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        # В первую очередь запрашиваем весь список реестров
        registries_list = requests.get(common.BASE_URL + REGISTRY_PIPE_SETTINGS["paths"]["get_registries_list"],
                                       headers=common.default_settings["access"]["headers"])
        if registries_list.ok:
            # если удалось запросить список реестров, то начинаем запрашивать реестры по одному
            with concurrent.futures.ThreadPoolExecutor() as executor:
                res = [executor.submit(single_registry_request, elem['registry_id']) for elem in registries_list.json()]
                concurrent.futures.wait(res)
            # просто копируем чужой код, чтобы не парсить самостоятельно :)
            depart_names, sample_numbers, values, registry_id = list(), list(), list(), list()
            for processed_concurrent in res:
                sample = processed_concurrent.result().json()
                for sampleRegister in sample['sampleRegistries']:
                    depart_names.append(sampleRegister['sample']['user']['depart']['depart_name'])
                    sample_numbers.append(sampleRegister['sample']['sample']['sample_number'])
                    values.append(sampleRegister['sample']['formValue']['sample_name']['value'])
                    registry_id.append(sampleRegister['registry_id'])
            # должна получиться таблица с реестрами
            csv = pd.DataFrame(
                data={'registry_id': registry_id, 'depart_name': depart_names,
                      'sample_number': sample_numbers, 'value': values})
            # сохраняем не через функцию из common, чтобы не нагромождать код зря
            csv.to_csv(path_registry_table, index=False, encoding="utf-8")
        else:
            raise AssertionError(f"Could not request registries list: {registries_list.status_code}:" +
                                 f" {registries_list.text}")
    except Exception as e:
        response['payload'] = str(e)
    else:
        # если все прошло успешно, то возвращаем таблицу с реестром
        response['success'] = True
        response['payload'] = csv

    return response


def read_all_registry_info(table_path: str) -> dict:
    """
    Считывание и проверка наименований столбцов таблицы, содержащей полную информацию о всех доступных реестрах.
    Так как исходно эта таблица записывается из другого DataFrame, то мы не объявляем заголовки столбцов мануально,
    но проверяем их, чтобы избежать KeyError в дальнейшем. \n \n
    :param table_path: путь к текстовому (по-умолчанию csv) файлу таблицы;
    :return: словарь вида STATE, payload - DataFrame с информацией о реестрах в случае успеха.
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        df = pd.read_csv(table_path, dtype=str, encoding="utf-8")
        assert df.columns.tolist() == REGISTRY_PIPE_SETTINGS["column_names"]["registry"]
    except Exception as e:
        response['payload'] = str(e)
    else:
        response['success'] = True
        response['payload'] = df

    return response


def create_regions_short_name(value: str) -> str:
    """
    Функция, которая используется как переходник для lambda-функции, позволяющая возвращать ошибку в случае отсутствия
    необходимого ключа. \n \n
    :param value: полное имя региона из таблицы Литеха;
    :return: имя из словаря в случае, если ключ был найден.
    """
    try:
        desired = REGISTRY_PIPE_SETTINGS['region_renames'][value]
    except KeyError:
        print(f"Необходимо добавить регион `{value}` в словарь сокращений")
        raise AssertionError
    else:
        return desired


def append_desired_columns(df: pd.DataFrame) -> dict:
    """
    Функция для расширения количества колонок до необходимого при дальнейшей работе пайплайна.
    Здесь добавляются вообще все столбцы, что нужны будут на всех этапах работы пайплайна.
    КАЗАЛОСЬ БЫ, что таблица генерируется максимально криво, но текущая генерация:
     * позволяет определять порядок столбцов нужным образом, выделять нужные из них;
     * избавляет нас от операции дополнительной и\или последующей неудобной аллокации памяти;
     * нравится мне. \n \n
    :param df: таблица для расширения;
    :return: словарь вида STATE, payload - DataFrame с обновленными колонками в случае успеха
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        # создаем колонку коротких наименований регионов
        df["region_short_name"] = df["litech_region"].apply(lambda x: create_regions_short_name(x))
        # объявляем сбор данных для будущей таблицы вида TABLE
        future_df = dict()
        for heading in REGISTRY_PIPE_SETTINGS["column_names"]["total"]:
            # если такое наименование есть в таблице баркодов и пересечений, то забираем значения оттуда
            if heading in df.columns:
                future_df[heading] = df[heading].to_list()
            # если такого наименования нет, то просто заполняем пустыми строками
            else:
                future_df[heading] = ["" for _ in range(df.index.size)]
        # создаем итоговую таблицу вида TABLE
        df_res = pd.DataFrame(future_df).set_index('barcode')  # устанавливаем баркод в качестве индекса
    except Exception as e:
        response['payload'] = str(e)
    # если не случилось исключений, то возвращаем её целиком
    else:
        response['success'] = True
        response['payload'] = df_res

    return response


def old_fashion_search(row, target_reg_df):
    """
    Функция для поиска номера реестра среди всех переданных реестров на основе наивного поиска подстроки в строке,
    приводит текстовое обозначение степени уверенности в корректном результате. \n \n
    :param row: строка, содержащая информацию по образце, в прошлом -- строка целой таблицы TABLE;
    :param target_reg_df: выборка таблицы с реестрами
    :return: pd.Series  -- результат поиска реестра
    """
    search_table = target_reg_df[target_reg_df['value'].apply(lambda x: row['litech_sample_name'] in x.lower()).tolist()]
    overlaps = search_table.shape[0]
    # начинаем сверку
    # если не найдено точных совпадений, проверим окончание на указанное значение
    if overlaps == 0:
        row['registry_guess_status'] = "NO MATHCES"
    # если найдено одно точное, то проверяем совпадение региона
    elif overlaps == 1:
        as_series = search_table.squeeze(axis=0)
        # если регион совпадает, то все пишем в порядке
        if as_series['sample_number'][:4] == row['region_short_name']:
            row[['registry_id', 'depart_name', 'sample_number', 'sample_name_value']] = as_series.tolist()
            if row['litech_sample_name'] == row['sample_name_value'].lower():
                row['registry_guess_status'] = "OK"
            else:
                row['registry_guess_status'] = "ALMOST OK"
        # в противном случае пишем об ошибке
        else:
            row['registry_guess_status'] = "REGION DOES NOT MATCH"
    # если обнаружено более двух совпадений, то нужно поискать совпадение региона
    else:
        sub_search_table = search_table[
            search_table['sample_number'].apply(lambda x: x[:4] == row['region_short_name']).tolist()]
        sub_overlaps = sub_search_table.shape[0]
        # если после уточнения региона записей нет, то что-то не так
        if sub_overlaps == 0:
            row['registry_guess_status'] = "NAME MATCHES BUT REGION DOES NOT"
        # если после уточнения региона осталась лишь одна запись, то все в порядке
        elif sub_overlaps == 1:
            row[['registry_id', 'depart_name',
                 'sample_number', 'sample_name_value']] = sub_search_table.squeeze(axis=0).tolist()
            if row['litech_sample_name'] == row['sample_name_value'].lower():
                row['registry_guess_status'] = "OK"
            else:
                row['registry_guess_status'] = "ALMOST OK"
        # если после уточнения региона записей больше одной, то сообщаем о дубликате
        else:
            row['registry_guess_status'] = "NAME AND REGION DUPLICATES"
    return row


# TODO: table_3 -- проверка уникальности 'litech_sample_name', иначе уведомление в статусе и остановка обработки образца
def process_table_concatenation(df: pd.DataFrame, df_registry: pd.DataFrame) -> dict:
    """
    Функция для поиска номера реестра среди всех реестров на основе наивного поиска подстроки в строке,
    приводит текстовое обозначение степени уверенности в корректном результате. \n \n
    :param df: таблица с образцами, для которых ведется поиск;
    :param df_registry: полная таблица реестров;
    :return: словарь вида STATE, payload - DataFrame с обновленными данными в случае успеха.
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        transform_func = lambda x: transliterate.translit(x, reversed=True).lower() if bool(
            re.search('[а-яА-Я]', x)) else x.lower()

        for barcode in df.index:
            standard_go = False  # флаг для инициации старого поиска

            # создаем поисковое имя, которое представляет преобразованное имя Литеха,
            # а именно -- без капитализации, без кириллицы и тд
            litech_clear_name = transform_func(df.loc[barcode, 'litech_sample_name'])
            # создаем поисковую клон-строку 
            row_clone = df.loc[barcode].copy(deep=True)
            row_clone['litech_sample_name'] = litech_clear_name  # присваиваем ей обработанное имя Литеха образца

            # обрабатываем предположение о реестре -- заменяем запятые на ';', избавляемся от любых букв и пробельных символов
            initial_guess_string = re.sub("[\s\D]+", "", re.sub(",", ";", df.loc[barcode, 'litech_registry_guess']))
            # сплитим такую строку по ';', извлекая подходящие нам id реестров
            sub_regs = df_registry[df_registry['registry_id'].isin(initial_guess_string.split(";"))]  # выделение
            # анализируем получившуюся выборку реестров
            if sub_regs.shape[0] != 0:
                # инициализируем поиск по выборке, передавая копию (!) клона и выборку
                ocd_res = old_fashion_search(row_clone.copy(deep=True), sub_regs)
                if (ocd_res['registry_guess_status'] == "OK") or (ocd_res['registry_guess_status'] == "ALMOST OK"):
                    # если отработало корректно, то получаем информацию из измененной копии клон-строки
                    df.loc[barcode, ['registry_id', 'depart_name', 'sample_number', 'sample_name_value',
                                     'registry_guess_status']] = ocd_res[
                                    ['registry_id', 'depart_name', 'sample_number', 'sample_name_value',
                                     'registry_guess_status']]
                else:
                    standard_go = True
            else:
                # если выборка пуста, то передаем флаг прогона через стандартный алгоритм
                standard_go = True
            # если мы добрались аж сюда, то значит предварительная догадка о реестре не привела к результатам
            if standard_go:
                # если код добрался сюда, то передавать измененную клон-строку нельзя, именно поэтому
                # ранее использовалась deep копия строки!
                ocd_res = old_fashion_search(row_clone.copy(deep=True), df_registry)
                # тут уже, как бы не отработало, сохраняем в результаты
                df.loc[barcode, ['registry_id', 'depart_name', 'sample_number', 'sample_name_value',
                                 'registry_guess_status']] = ocd_res[
                                ['registry_id', 'depart_name', 'sample_number', 'sample_name_value',
                                 'registry_guess_status']]
    # здесь мало представляю, как может появиться Exception, но все же...
    except Exception as e:
        response['payload'] = str(e)
    else:
        response['success'] = True
        response['payload'] = df

    return response
