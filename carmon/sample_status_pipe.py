"""
Раздел, отвечающий за проставление статуса результатам
"""
import json

import pandas as pd
import requests
from Bio import SeqIO

from . import common


SAMPLE_STATUS_DICT = common.load_config(f"{common.WORKING_PATH}/sample_status_pipe_settings.yaml")


def request_sample_status_types():
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
                comparison_dict[elem['text']] = elem['value']
            if comparison_dict == SAMPLE_STATUS_DICT["status"]["vga_status_types"]:
                response['success'] = True
                response['payload'] = comparison_dict
            else:
                # здесь будет возвращаться False и новый словарь, в соответствии с которым нужно провести
                # обновление вариантов статусов
                response['payload'] = comparison_dict
        else:
            response['payload'] = f"{vga_request.status_code}: {vga_request.text}`"
    pass


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
                if (df.loc[barcode, 'registry_guess_status'] == "OK") or (
                        df.loc[barcode, 'registry_guess_status'] == "ALMOST OK"):
                    df.loc[barcode, 'sample_status_local'] = 'Готов'
                else:  # если с реестром проблемы, то объявляем об этом в локальном заключении
                    # проставляем такой статус, чтобы можно было в дальнейшем взаимодействовать с этим вручную
                    df.loc[barcode, 'sample_status_local'] = 'NOT READY'
            else:  # если последовательность невалидная, то проставляем 'Брак сиквенса'
                df.loc[barcode, 'sample_status_local'] = 'Брак сиквенса'
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
        sub_df = df[df["sample_status_local"].isin(SAMPLE_STATUS_DICT["status"])]
        barcodes = sub_df.index.tolist()
        idx = 0
        # станем итерироваться по increment образцов
        while idx < len(barcodes):
            # получаем срез списка имен образцов
            concatenated_sample_numbers = sub_df.loc[barcodes[idx:idx+increment], 'sample_number'].tolist()
            # запрашиваем информацию об образцах POST-запросом
            samples_info = requests.post(common.BASE_URL + SAMPLE_STATUS_DICT["paths"]["status_types"],
                                         headers=common.default_settings["access"]["headers"],
                                         data={
                                             json.dumps({"filter": concatenated_sample_numbers})
                                         })
            # если запрос прошел корректно, то обрабатываем результаты
            if samples_info.status_code == 200:
                for row in samples_info.json():
                    # получаем имена образцов и сравниваем их с уже записанными,
                    # чтобы убедиться в корректной последовательности образцов в ответе сервера
                    ali_index = df[df['sample_number'] == row['sample_number']].index
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


def upload_sequence(df: pd.DataFrame, fasta_upload: dict, credentials: dict) -> dict:
    """
    Загрузка сиквенсов на сервер. \n \n
    :return:
    """
    response = common.DEFAULT_RESPONSE.copy()

    return response


def state_sample_status_remote(df: pd.DataFrame, increment: int = 40) -> dict:
    """
    Отправка локальных статусов образцов на сервер.
    :return:
    """
    response = common.DEFAULT_RESPONSE.copy()
    # сперва проставляем значения "брак сиквенса"

    # затем загружаем сиквенсы и проставляем "Готов" для тех, кого удалось загрузить
    return response


def check_sample_status_success():
    """
    Првоерка корректности выставления статусов образцов. \n \n
    :return:
    """
    pass
