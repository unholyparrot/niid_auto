"""
Раздел для выставления заключения загруженным образцам
"""
# TBD
import requests
import yaml

from . import common


with open(f"{common.WORKING_PATH}/conclusion_pipe_settings.yaml", "r", encoding="utf-8") as fr:
    CONCLUSION_PIPE_SETTINGS = yaml.load(fr, Loader=yaml.SafeLoader)

BASE_PATH = CONCLUSION_PIPE_SETTINGS["paths"]["base"]


def remember_token(token: str) -> dict:
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
        test_request = requests.get(BASE_PATH + CONCLUSION_PIPE_SETTINGS["paths"]["ping"],
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
    :return: 
    """
    pass


def read_and_prepare_data() -> dict:
    """
    Функция для прочтения входных данных и их подготовки. Под входными данными подразумевается таблица вида FULL_TABLE,
    файл с результатами работы Pangolin по этим образцам и файл с результатами работы NextClade по этим образцам.
    Проходит проверка соответствия имен образцов и дополнение первичной таблицы данных результатами работы
    сторонних программ. \n \n 
    :return:
    """
    response = common.DEFAULT_RESPONSE.copy()
    return response


def request_samples_info():
    """
    Функция для запроса информации об образцах на основе их имен. 
    Необходимо учитывать, что сервер не предоставляет информации больше, 
    чем для 50 образцов, ввиду ограничений размеров таблицы, поэтому неизбежен цикличный запрос информации.
    По этой же причине придется сразу после запроса информации проводить парсинг результатов и обновление таблицы, 
    так как нужно где-то хранить промежуточные результатами между запросами по <50 образцов. 
    Все локальные ошибки единичных образцов, т.е. не критических, будут записаны в поле 'status' таблицы.
    Критические же ошибки (например, `500 Server Error`) должны останавливать работу всей функции.  \n \n 
    :return:
    """
    pass


def state_conclusion_local():
    """
    Функция для определения заключения по сводным результатам Pangolin и NextClade. Не отправляет запроса на сервер,
    но локально подтягивает заключение и его соответствие возможным вариантам на сервере. \n \n 
    :return: 
    """
    pass


def state_conclusion_remote():
    """
    Функция для отправки результатов заключений на сервер. Аналогично запросу образцов, необходимо поэтапное (по 50 
    образцов) выставление результатов. \n \n 
    :return: 
    """
    pass


def check_conclusion_success():
    """
    Функция для проверки успеха проставления заключений на сайте. В случае полного соответствия проставляет 'OK' в поле
    'status' итоговой таблицы. Производит поэтапные запросы (по 50 образцов). 
    :return: 
    """
    pass
