"""
Раздел, отвечающий за составление сборной таблицы образцов по входным таблицам Литеха
и внутренним таблица из Google Sheets.
Представленному разделу соответствует `registry_pipe_settings.yaml` с некоторыми константами, а также
ноутбук registry_pipe_example.ipynb в качестве образца работы.
"""
import pandas as pd

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
        # создаем баркоды
        barcodes = df2["Dispence_to"].apply(lambda x: f"{x}" if len(x) > 1 else f"0{x}")
        # и словарь соответствий баркода штрихкоду Литеха
        barcodes_dict = {x: y for x, y in zip(df2["Sample_name"], barcodes)}
        df3 = pd.read_csv(table_3_path,
                          sep=separator, dtype=str, encoding="utf-8",
                          names=REGISTRY_PIPE_SETTINGS["column_names"]["from_2"])
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
            response['payload'] = "Не совпадает число образцов в плашке и число найденных образцов в таблице образцов"

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
        df_res = pd.DataFrame(future_df)
    except Exception as e:
        response['payload'] = str(e)
    # если не случилось исключений, то возвращаем её целиком
    else:
        response['success'] = True
        response['payload'] = df_res

    return response


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
        # здесь было бы хорошо добавить tqdm для оценки времени работы, но, так как это будет импортироваться,
        # я не хочу загрязнять потенциальные логи чужой консоли, обойдемся без индикатора прогресса
        # итерируемся по строкам таблицы
        for idx, row in df.iterrows():
            # проверяем наличие имени от Литеха в имени образца реестра
            search_table = df_registry[df_registry['value'].apply(lambda x: row['litech_sample_name'] in x).tolist()]
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
                    if row['litech_sample_name'] == row['sample_name_value']:
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
                    if row['litech_sample_name'] == row['sample_name_value']:
                        row['registry_guess_status'] = "OK"
                    else:
                        row['registry_guess_status'] = "ALMOST OK"
                # если после уточнения региона записей больше одной, то сообщаем о дубликате
                else:
                    row['registry_guess_status'] = "NAME AND REGION DUPLICATES"
    # здесь мало представляю, как может появиться Exception, но все же...
    except Exception as e:
        response['payload'] = str(e)
    else:
        response['success'] = True
        response['payload'] = df

    return response
