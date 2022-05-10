import abc
import datetime
import json
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None) -> None:
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        """
        Сохранение текущего состояния

        :param state: текущее состояние для сохранения
        :return: None
        """
        with open(self.file_path, 'w') as fp:
            json.dump(state, fp)

    def retrieve_state(self) -> dict:
        """
        Получение текущего состояния

        :return: словарь состояния
        """
        if not self.file_path:
            return {}
        try:
            with open(self.file_path, 'r') as fp:
                state = json.load(fp)
            return state
        except FileNotFoundError:
            self.save_state({})
        except json.decoder.JSONDecodeError:
            return {}


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage
        self.data = storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.data.get(key) if self.data.get(key) else datetime.datetime.min
