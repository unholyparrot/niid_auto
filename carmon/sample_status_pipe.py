"""
Раздел, отвечающий за проставление статуса результатам
"""
import os
import base64
import json
import shutil
import textwrap
import tempfile
import datetime

import pandas as pd
import requests
from Bio import SeqIO

from . import common


SAMPLE_STATUS_DICT = common.load_config(f"{common.WORKING_PATH}/sample_status_pipe_settings.yaml")


def request_sample_status_types() -> dict:
    response = common.DEFAULT_RESPONSE.copy()
    try:
        vga_request = requests.get(common.BASE_URL + SAMPLE_STATUS_DICT["paths"]["status_types"],
                                   headers=common.default_settings["access"]["headers"])
    except Exception as e:
        response['payload'] = str(e)
    else:
        if vga_request.status_code == 200:
            comparison_dict = dict()
            for elem in vga_request.json():
                comparison_dict[elem['text']] = elem['id']
            if comparison_dict == SAMPLE_STATUS_DICT["status"]["vga_status_types"]:
                response['success'] = True
                response['payload'] = comparison_dict
            else:
                # здесь будет возвращаться False и новый словарь, в соответствии с которым нужно провести
                # обновление вариантов статусов
                response['payload'] = comparison_dict
        else:
            response['payload'] = f"{vga_request.status_code}: {vga_request.text}`"
    return response


def state_sample_status_local(df: pd.DataFrame, fasta_path: str) -> dict:
    """
    Выставление локального заключения о качестве сиквенса для образца. \n \n
    :param df:
    :param fasta_path:
    :return:
    """
    response = common.DEFAULT_RESPONSE.copy()
    future_upload = dict()
    try:
        with open(fasta_path, "r", encoding="utf-8") as fasta_file:  # открываем Fasta
            for seq_record in SeqIO.parse(fasta_file, 'fasta'):  # начинаем итерацию по последовательностям
                # получаем из FASTA имя последовательности и превращаем в баркод
                current_barcode = f"{str(seq_record.id)}_MN908947.3"
                # фактически просто игнорируем те результаты последовательности, что не входят в плашку
                if current_barcode in df.index:
                    # вычисляем ATGC состав
                    atgc_count = sum(seq_record.seq.count(x) for x in ["A", "T", "G", "C"])
                    # определяем, валидна ли последовательность
                    valid = atgc_count > SAMPLE_STATUS_DICT["THRESHOLD"]
                    # выставляем полученное значение в таблицу
                    df.loc[current_barcode, 'valid_seq'] = valid
                    # для валидных последовательностей сохраняем FASTA
                    future_upload[current_barcode] = str(seq_record.seq)
        # проверяем, не появилось ли каких-то лишних записей
        if df[df['valid_seq'] == ""].shape[0] != 0:
            raise AssertionError(f"Не обнаружены в Fasta-файле: {', '.join(df[df['valid_seq'] == ''].index)}")
        # выставляем локальное заключение на основании угадывании реестра и качестве последовательности
        for barcode in df.index:
            if df.loc[barcode, 'valid_seq']:  # если последовательность валидная
                # Проверяем, что там по реестру последовательности, т.к. иначе не сможем загрузить
                if df.loc[barcode, 'registry_guess_status'] == "OK":
                    df.loc[barcode, 'sample_status_local'] = 'Готов'
                else:  # если с реестром проблемы, то объявляем об этом в локальном заключении
                    # проставляем такой статус, чтобы можно было в дальнейшем взаимодействовать с этим вручную
                    df.loc[barcode, 'sample_status_local'] = 'Требуется подтверждение'
            else:  # если последовательность невалидная, то проставляем 'Брак сиквенса'
                # Проверяем, что там по реестру последовательности, т.к. иначе не сможем поставить статус
                if df.loc[barcode, 'registry_guess_status'] == "OK":
                    df.loc[barcode, 'sample_status_local'] = 'Брак сиквенса'
                else:  # если с реестром проблемы, то объявляем об этом в локальном заключении
                    # проставляем такой статус, чтобы можно было в дальнейшем взаимодействовать с этим вручную
                    df.loc[barcode, 'sample_status_local'] = 'Требуется подтверждение'
    except Exception as e:
        response['payload'] = str(e)
    else:
        response['success'] = True
        response['payload'] = df, future_upload

    return response


def request_samples_info(df: pd.DataFrame, increment: int = 40):
    """
    Получение информации об образцах для выяснения их 'истинных' id, по которым в дальнейшем можно проставить статус
    образца. \n \n
    :param increment:
    :param df:
    :return:
    """
    response = common.DEFAULT_RESPONSE.copy()
    try:
        # тут хитрый момент, мы запрашиваем ID лишь для тех образцов,
        # которые были определены как подходящие для выставления хоть какого-то статуса
        sub_df = df[df["sample_status_local"].isin(set(SAMPLE_STATUS_DICT["status"]["vga_status_types"]))]
        barcodes = sub_df.index.tolist()
        idx = 0
        # станем итерироваться по increment образцов
        while idx < len(barcodes):
            # получаем срез списка имен образцов
            concatenated_sample_numbers = sub_df.loc[barcodes[idx:idx+increment], 'sample_number'].tolist()
            # запрашиваем информацию об образцах POST-запросом
            samples_info = requests.post(common.BASE_URL + SAMPLE_STATUS_DICT["paths"]["samples_info"],
                                         headers=common.default_settings["access"]["headers"],
                                         data=json.dumps({"filter": concatenated_sample_numbers}))
            # если запрос прошел корректно, то обрабатываем результаты
            if samples_info.status_code == 200:
                for row in samples_info.json():
                    # получаем имена образцов и сравниваем их с уже записанными,
                    # чтобы убедиться в корректной последовательности образцов в ответе сервера
                    ali_index = df[df['sample_number'] == row['sample']['sample_number']].index
                    # каждому образцу должен соответствовать лишь один
                    if len(ali_index) == 1:
                        df.loc[ali_index, 'sample_vga_id'] = row['id']
                    # если соответствует более чем один, то сообщаем об ошибке
                    else:
                        raise AssertionError(f"Найдено {len(ali_index)} соответствий для " +
                                             f"`{row['sample_number']}` (id {row['id']})")
            # если запрос обработать не удалось, то также поднимаем ошибку
            else:
                raise AssertionError(f"Request for {idx}:{idx + increment} failed with {samples_info.status_code}")
            # после успешной итерации увеличиваем счетчик
            idx += increment
        # проверяем, все ли из выбранных образцов получили свои ID
        sub_df = df[df["sample_status_local"].isin(set(SAMPLE_STATUS_DICT["status"]["vga_status_types"]))]
        cur_counter = sub_df[sub_df['sample_vga_id'] == ""].shape[0]
        if cur_counter != 0:
            raise AssertionError(f"Как минимум один ({cur_counter}) из образцов не получил id с портала")
    # возникшие ошибки обрабатываем
    except Exception as e:
        response['payload'] = str(e)
    else:
        response['success'] = True
        response['payload'] = df

    return response


def upload_sequences(df: pd.DataFrame, fasta_upload: dict, credentials: dict, archive_path: str) -> dict:
    """
    Загрузка сиквенсов на сервер. Выбирает из TABLE те записи, для которых локальный статус выставлен
    'Готов'. Не совершает никаких действий с теми образцами, что имеют иные статусы. \n \n
    :param df:
    :param fasta_upload:
    :param credentials:
    :param archive_path:
    :return:
    """
    response = common.DEFAULT_RESPONSE.copy()

    tmp_fasta_path = tempfile.mkdtemp()
    ts_mark = datetime.datetime.now()

    token = base64.b64encode(f"{credentials['login']}:{credentials['password']}".encode()).decode()
    special_headers = {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json"
    }
    operation_status = True
    for barcode in df.index:
        if df.loc[barcode, 'sample_status_local'] == 'Готов':
            try:
                # тут, вообще говоря, надо подумать, как все красиво спихнуть в конфигурацию
                beautiful_fasta = "\n".join(textwrap.wrap(fasta_upload.get(barcode), 60))
                single_sample = {
                    'sample_number': df.loc[barcode, 'sample_number'],
                    'sample_data': {
                        'sequence_name': df.loc[barcode, 'litech_barcode'],
                        'sample_type': '1',
                        'seq_area': '1',
                        'author': 'Говорун В.М.',
                        'genom_pick_method': 'nf_artic',
                        'method_ready_lib': 'MIDNIGHT',
                        'tech': '3',
                        'valid': True,
                        'seq_id': df.loc[barcode, 'sample_name_value']  # опциональный параметр
                    },
                    'sequence': f">DEZIN-{df.loc[barcode, 'litech_barcode']}\n{beautiful_fasta}"
                }
                single_upload = requests.post(common.BASE_URL + SAMPLE_STATUS_DICT["paths"]["upload"],
                                              headers=special_headers,
                                              data=json.dumps([single_sample]))
                if single_upload.status_code == 200:
                    df.loc[barcode, 'sample_status_remote'] = 'Uploaded'
                else:
                    df.loc[barcode, 'sample_status_remote'] = f"Failed with " \
                                                              f"{single_upload.status_code}:{single_upload.text}"
                    operation_status = False
            # возникшие ошибки обрабатываем
            except Exception as e:
                df.loc[barcode, 'sample_status_remote'] = f"Failed with {str(e)}"
                operation_status = False
            else:
                with open(os.path.join(tmp_fasta_path, f"dezin-{df.loc[barcode, 'litech_barcode']}.fasta"), "w") as ff:
                    ff.write(f">DEZIN-{df.loc[barcode, 'litech_barcode']}\n{beautiful_fasta}")

    with open(os.path.join(tmp_fasta_path, f'{ts_mark.strftime("%y%m%d_%H%M")}_upload_report.txt'), "w") as ts_wr:
        ts_wr.write(f"Upload start\t{ts_mark.strftime('%Y-%m-%d %H:%M')}\n")
        ts_wr.write(f"Attempted to upload\t{df[df['sample_status_local'] == 'Готов'].shape[0]}\n")
        ts_wr.write(f"Succeeded to upload\t{df[df['sample_status_remote'] == 'Uploaded'].shape[0]}\n")
        ts_wr.write(f"Upload finish\t{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    common.make_archive(tmp_fasta_path, archive_path)
    shutil.rmtree(tmp_fasta_path)

    response['success'] = operation_status
    response['payload'] = df

    return response


def state_sample_status_remote(df: pd.DataFrame, increment: int = 40, status: str = 'Брак сиквенса') -> dict:
    """
    Отправка локальных статусов STATUS образцов на сервер.
    :return:
    """
    response = common.DEFAULT_RESPONSE.copy()

    try:
        if status not in set(SAMPLE_STATUS_DICT["status"]["vga_status_types"]):
            raise AssertionError(f"Неизвестный статус `{status}`")
        sub_df = df[df['sample_status_local'] == status]
        barcodes = sub_df.index.tolist()
        idx = 0
        # станем итерироваться по increment образцов
        while idx < len(barcodes):
            # получаем срез списка имен образцов
            concatenated_sample_ids = sub_df.loc[barcodes[idx:idx + increment], 'sample_vga_id'].tolist()
            # отправляем статус образцов POST-запросом
            status_change = requests.post(common.BASE_URL + SAMPLE_STATUS_DICT["paths"]["status_change"],
                                          headers=common.default_settings["access"]["headers"],
                                          files={
                                              "uploads": (None, ",".join(map(str, concatenated_sample_ids))),
                                              "status": (None,
                                                         str(SAMPLE_STATUS_DICT["status"]["vga_status_types"][status])),
                                              "defect_id": (None, ''),
                                              "auth_key": (None, common.default_settings["access"]["token"])
                                          })
            # если запрос прошел корректно, то обрабатываем результаты
            if status_change.status_code == 200:
                # в ответе должно быть True\False
                if status_change.json():
                    # если все ок, то записываем это в результат выставления
                    df.loc[barcodes, 'sample_status_remote'] = "Проставлено"
                else:
                    raise AssertionError(f"Не удалось выставить статус для {idx}:{idx + increment}")
            # если запрос обработать не удалось, то также поднимаем ошибку
            else:
                raise AssertionError(f"Request for {idx}:{idx + increment} failed with " +
                                     f"{status_change.status_code}:{status_change.text}")
            # после успешной итерации увеличиваем счетчик
            idx += increment
        # проверяем, всем ли из выбранных образцов удалось проставить статус
        sub_df = df[df['sample_status_local'] == status]
        cur_counter = sub_df[sub_df['sample_status_remote'] == ""].shape[0]
        if cur_counter != 0:
            raise AssertionError(f"Как минимум одному ({cur_counter}) образцу не удалось выставить статус")
    # возникшие ошибки обрабатываем
    except Exception as e:
        response['payload'] = str(e)
    else:
        response['success'] = True
        response['payload'] = df

    return response


# TODO: добавить функцию повторной загрузки fasta-файла для уже загруженного
def repost_sample_sequence():
    pass


# TODO: реализовать проверку успеха загрузки и выставления статусов
def check_sample_status_success():
    """
    Првоерка корректности выставления статусов образцов. \n \n
    :return:
    """
    pass
