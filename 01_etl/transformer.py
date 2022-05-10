from config import index_name
from models import Movie


class Transformer:
    def prepare_entries(self, data):
        entries = list()
        for idx, rating, genre, title, descr, director, actors_n, writers_n, actors, writers in data:
            entry = Movie(
                id=idx, imdb_rating=rating, genre=genre, title=title,
                description=descr, director=director, actors_names=actors_n,
                writers_names=writers_n, actors=actors, writers=writers)
            entries.append(entry)
        return entries

    def compile_data(self, data):
        entries = self.prepare_entries(data)

        out = list()
        for entry in entries:
            index_template = {"index": {"_index": index_name, "_id": str(entry.id)}}
            data_template = {
                "id": str(entry.id), "imdb_rating": entry.imdb_rating,
                "genre": entry.genre, "title": entry.title,
                "description": entry.description, "director": entry.director,
                "actors_names": entry.actors_names, "writers": entry.writers,
                "writers_names": entry.writers_names, "actors": entry.actors,}
            out.append(index_template)
            out.append(data_template)

        return out
