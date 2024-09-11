from typing import Dict, List

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


class Classifier:
    """
    A class representing a classifier.

    Methods:
        get_instance(model_name): Returns an instance of the classifier for the given model name.
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
            cls._instances[model_name] = {
                "tokenizer": AutoTokenizer.from_pretrained(model_name),
                "model": AutoModelForSequenceClassification.from_pretrained(model_name),
            }
        return cls._instances[model_name]


def get_classifier(model_name):
    """
    Returns an instance of the Classifier class for the specified model name.

    Parameters:
        model_name (str): The name of the model.

    Returns:
        Classifier: An instance of the Classifier class.
    """
    return Classifier.get_instance(model_name)


def convert_finbert_output(
    softmax_output: List[float], id2label: Dict[int, str]
) -> float:
    """
    Converts the softmax output of FinBERT model into a weighted sum based on sentiment labels.

    Args:
        softmax_output (List[float]): The softmax output of the FinBERT model.
        id2label (Dict[int, str]): A dictionary mapping label IDs to sentiment labels.

    Returns:
        float: The weighted sum of the softmax output based on sentiment labels.
    """
    # Define weights for each sentiment
    weights = {"positive": 1, "negative": -1, "neutral": 0}

    # Calculate weighted sum
    weighted_sum = sum(
        softmax_output[i] * weights[id2label[i]] for i in range(len(softmax_output))
    )

    return weighted_sum


def sentiment_score(text: str, model_name: str = "ProsusAI/finbert") -> float:
    """
    Calculates the sentiment score of the given text using the specified model.

    Args:
        text (str): The input text for sentiment analysis.
        model_name (str, optional): The name of the model to use for sentiment analysis.
            Defaults to "ProsusAI/finbert".

    Returns:
        float: The sentiment score of the input text in the range [-1, 1].
    """

    classifier = get_classifier(model_name)
    inputs = classifier["tokenizer"](text, return_tensors="pt").to(DEVICE)
    model = classifier["model"].to(DEVICE)
    logits = model(**inputs).logits
    softmax_output = logits.softmax(dim=-1).detach().numpy().ravel().tolist()

    return convert_finbert_output(
        softmax_output=softmax_output, id2label=model.config.id2label
    )
