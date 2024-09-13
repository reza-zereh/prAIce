import torch
from sentence_transformers import SentenceTransformer

from ..utils import cosine_similarity


class SentenceSimilarity:
    """
    A class representing a sentence similarity model.

    Methods:
        get_instance(model_name): Returns an instance of the sentence similarity model for the given model name.
    """

    _instances = {}

    @classmethod
    def get_instance(cls, model_name):
        """
        Returns an instance of the specified model_name if it exists, otherwise creates a new instance and returns it.

        Parameters:
            - model_name: The name of the model.

        Returns:
            - An instance of the specified model_name.
        """
        if model_name not in cls._instances:
            cls._instances[model_name] = SentenceTransformer(
                model_name,
                device=torch.device("cuda" if torch.cuda.is_available() else "cpu"),
            )
        return cls._instances[model_name]


def get_similarity_model(model_name) -> SentenceTransformer:
    """
    Returns an instance of the SentenceSimilarity class for the specified model name.

    Parameters:
        model_name (str): The name of the model.

    Returns:
        SentenceSimilarity: An instance of the SentenceSimilarity class.
    """
    return SentenceSimilarity.get_instance(model_name)


def similarity_score(
    sentence1, sentence2, model_name: str = "sentence-transformers/all-mpnet-base-v2"
) -> float:
    """
    Returns the similarity score between two sentences using the specified model.

    Parameters:
        sentence1 (str): The first sentence to compare.
        sentence2 (str): The second sentence to compare.
        model_name (str): The name of the model to use for sentence similarity.

    Returns:
        float: The cosine similarity score between the two sentences.
    """
    model = get_similarity_model(model_name)
    embeddings = model.encode([sentence1, sentence2])
    return cosine_similarity(embeddings[0], embeddings[1])
