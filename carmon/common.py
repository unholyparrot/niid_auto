"""
Здесь предполагается размещение общих для всех компонентов констант
"""
import yaml
import requests

from os.path import split as split_it


def load_config(cfg_path: str) -> dict:
    """
    Для загрузки в память словарей-конфигураторов. \n \n
    :param cfg_path:
    :return:
    """
    with open(cfg_path, "r", encoding="utf-8") as fr:
        response = yaml.load(fr, Loader=yaml.SafeLoader)
    return response


WORKING_PATH = split_it(__file__)[0]

default_settings = load_config(f"{WORKING_PATH}/common_settings.yaml")

DEFAULT_RESPONSE = default_settings['default_response']
BASE_URL = default_settings['paths']['base']


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
        # обязательно пишем все в utf-8, чтобы не было в дальнейшем проблем
        pd_table.to_csv(output_name, sep=separator, encoding="utf-8")
    except Exception as e:
        response['payload'] = str(e)
    else:
        response['success'] = True
        response['payload'] = f"Успешно сохранено в `{output_name}`"

    return response


def state_token(token) -> dict:
    """
    Функция для внесения токена авторизации и дальнейшего доступа на портал.
    Запрашивает информацию о пользователе в текущей сессии, если это не удается, значит токен введен некорректно. \n \n
    :param token: токен авторизации вида `58bac2b5851a4a7a832c30a02271045ef7b476e599134a19bc159e3ff7468e31`;
    :return: словарь вида STATE, payload - информация об авторизированном пользователе в случае успеха.
    """
    response = DEFAULT_RESPONSE.copy()
    try:
        default_settings["access"]["token"] = token
        default_settings["access"]["headers"]["Authorization"] = f"Bearer {token}"
        test_request = requests.get(BASE_URL + default_settings['paths']['ping'],
                                    headers=default_settings["access"]["headers"])
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
