"""
Здесь предполагается размещение общих для всех компонентов констант
"""
from os.path import split as split_it


WORKING_PATH = split_it(__file__)[0]

DEFAULT_RESPONSE = {
    "success": False,
    "payload": "Default response definitely means an error"
}


# TODO: необходимо уточнить, что именно выступает в качестве payload в случае успеха этой функции
def save_concatenated_table(pd_table, output_name, separator='\t'):
    """
    Функция для сохранения сборной таблицы по указанному пути \n \n
    :param pd_table: таблица для сохранения;
    :param output_name: путь и имя для итогового файла;
    :param separator: разделитель в текстовом файле;
    :return: словарь вида STATE, payload - путь к сохраненному файлу в случае успеха.
    """
    response = DEFAULT_RESPONSE.copy()
    try:
        pd_table.to_csv(output_name, sep=separator)
    except Exception as e:
        response['payload'] = str(e)
    else:
        response['success'] = True
        response['payload'] = f"Успешно сохранено в `{output_name}`"

    return response
