"""
Раздел, отвечающий за составление сборной таблицы образцов по входным таблицам Литеха
и внутренним таблица из Google Sheets.
Представленному разделу соответствует `registry_pipe_settings.yaml` с некоторыми константами, а также
ноутбук registry_pipe_example.ipynb в качестве образца работы.
"""
import yaml
import pandas as pd

from . import constants


with open(f"{constants.WORKING_PATH}/registry_pipe_settings.yaml", "r", encoding="utf-8") as fr:
    REGISTRY_PIPE_SETTINGS = yaml.load(fr, Loader=yaml.SafeLoader)


# TODO: в дальнейшем необходимо добавить проверку имен столбцов после их стандартизации
def read_input_tables(table_2_path: str, table_3_path: str, separator='\t') -> dict:
    """
    Функция для загрузки в память входных таблиц, с которыми ведется работа. \n \n
    :param table_2_path: путь к таблице, содержащей данные из Таблицы 2 (НИИД, Google Sheets);
    :param table_3_path: путь к таблице, содержащей данные из Таблицы 3 (Литех, Google Sheets);
    :param separator: разделитель данных в текстовых файлах;
    :return: словарь вида STATE, payload - DataFrame с пересечением таблиц в случае успеха
    """
    response = constants.DEFAULT_RESPONSE.copy()
    try:
        df2 = pd.read_csv(table_2_path, sep=separator, dtype=str)
        df3 = pd.read_csv(table_3_path, sep=separator, dtype=str)
        df_res = df3[df3["litex_sk"].isin(df2["Sample_name"])].copy(deep=True)
    except Exception as e:
        response['payload'] = str(e)
    else:
        if df_res.shape[0] == df2.shape[0]:
            response['success'] = True
            response['payload'] = df_res
        else:
            response['payload'] = "Не совпадает число образцов в плашке и число найденных образцов в таблице образцов"

    return response


# TODO: казалось бы, к чему на чтение файла отдельная функция, но необходимо будет устраивать проверки столбцов
def read_all_registry_info(table_path: str) -> dict:
    """
    Считывание и проверка наименований столбцов таблицы, содержащей полную информацию о всех доступных реестрах. \n \n
    :param table_path: путь к текстовому (по-умолчанию csv) файлу таблицы;
    :return: словарь вида STATE, payload - DataFrame с информацией о реестрах в случае успеха.
    """
    response = constants.DEFAULT_RESPONSE.copy()
    try:
        df = pd.read_csv(table_path, dtype=str)
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


def append_desired_columns(pd_table: pd.DataFrame) -> dict:
    """
    Функция для расширения количества колонок до необходимого при дальнейшей работе пайплайна. \n \n
    :param pd_table: таблица для расширения;
    :return: словарь вида STATE, payload - DataFrame с обновленными колонками в случае успеха
    """
    response = constants.DEFAULT_RESPONSE.copy()
    try:
        pd_table["region_code"] = pd_table["region"].apply(lambda x: create_regions_short_name(x))
        for col_heading in REGISTRY_PIPE_SETTINGS['appending_columns']:
            pd_table[col_heading] = ""
    except Exception as e:
        response['payload'] = str(e)
    else:
        response['success'] = True
        response['payload'] = pd_table

    return response


def process_table_concatenation(pd_table: pd.DataFrame, pd_registry: pd.DataFrame) -> dict:
    """
    Функция для поиска номера реестра среди всех реестров на основе наивного поиска подстроки в строке,
    приводит текстовое обозначение степени уверенности в корректном результате. \n \n
    :param pd_table: таблица с образцами, для которых ведется поиск;
    :param pd_registry: полная таблица реестров;
    :return: словарь вида STATE, payload - DataFrame с обновленными данными в случае успеха.
    """
    response = constants.DEFAULT_RESPONSE.copy()
    try:
        # здесь было бы хорошо добавить tqdm для оценки времени работы, но, так как это будет импортироваться,
        # я не хочу загрязнять потенциальные логи чужой консоли, обойдемся без индикатора прогресса
        # итерируемся по строкам таблицы
        for idx, row in pd_table.iterrows():
            # проверяем наличие имени от Литеха в имени образца реестра
            search_table = pd_registry[pd_registry['value'].apply(lambda x: row['litex_value'] in x).tolist()]
            overlaps = search_table.shape[0]
            # начинаем сверку
            # если не найдено точных совпадений, проверим окончание на указанное значение
            if overlaps == 0:
                row['status'] = "NO MATHCES"
            # если найдено одно точное, то проверяем совпадение региона
            elif overlaps == 1:
                as_series = search_table.squeeze(axis=0)
                # если регион совпадает, то все пишем в порядке
                if as_series['sample_number'][:4] == row['region_code']:
                    row[['registry_id', 'depart_name', 'sample_number', 'value']] = as_series.tolist()
                    if row['litex_value'] == row['value']:
                        row['status'] = "OK"
                    else:
                        row['status'] = "ALMOST OK"
                # в противном случае пишем об ошибке
                else:
                    row['status'] = "REGION DOES NOT MATCH"
            # если обнаружено более двух совпадений, то нужно поискать совпадение региона
            else:
                sub_search_table = search_table[
                    search_table['sample_number'].apply(lambda x: x[:4] == row['region_code']).tolist()]
                sub_overlaps = sub_search_table.shape[0]
                # если после уточнения региона записей нет, то что-то не так
                if sub_overlaps == 0:
                    row['status'] = "NAME MATCHES BUT REGION DOES NOT"
                # если после уточнения региона осталась лишь одна запись, то все в порядке
                elif sub_overlaps == 1:
                    row[['registry_id', 'depart_name', 'sample_number', 'value']] = sub_search_table.squeeze(
                        axis=0).tolist()
                    if row['litex_value'] == row['value']:
                        row['status'] = "OK"
                    else:
                        row['status'] = "ALMOST OK"
                # если после уточнения региона записей больше одной, то сообщаем о дубликате
                else:
                    row['status'] = "NAME AND REGION DUPLICATES"
    # здесь мало представляю, как может появиться Exception, но все же...
    except Exception as e:
        response['payload'] = str(e)
    else:
        response['success'] = True
        response['payload'] = pd_table

    return response


# TODO: необходимо уточнить, что именно выступает в качестве payload в случае успеха этой функции
def save_concatenated_table(pd_table, output_name, separator='\t'):
    """
    Функция для сохранения сборной таблицы по указанному пути \n \n
    :param pd_table: таблица для сохранения;
    :param output_name: путь и имя для итогового файла;
    :param separator: разделитель в текстовом файле;
    :return: словарь вида STATE, payload - путь к сохраненному файлу в случае успеха.
    """
    response = constants.DEFAULT_RESPONSE.copy()
    try:
        pd_table.to_csv(output_name, sep=separator)
    except Exception as e:
        response['payload'] = str(e)
    else:
        response['success'] = True
        response['payload'] = f"Успешно сохранено в `{output_name}`"

    return response
