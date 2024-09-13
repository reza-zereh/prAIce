from fastapi import FastAPI

from .libs.sentiment import sentiment_score
from .libs.similarity import similarity_score
from .libs.summarizer import summarize
from .schema import SentimentRequest, SimilarityRequest, SummarizationRequest

app = FastAPI()


@app.post("/summarize")
async def summarize_route(request: SummarizationRequest):
    """
    Summarizes the given text using a specified model.

    Args:
        request (SummarizationRequest):
            The request object containing the model name and text to be summarized.

    Returns:
        dict: A dictionary containing the summary of the text.
    """
    summary = summarize(
        text=request.text, model_name=request.model_name, max_tokens=request.max_tokens
    )
    return {"summary": summary}


@app.post("/sentiment_score")
async def sentiment_score_route(request: SentimentRequest):
    """
    Calculates the sentiment score of the given text using a specified model.

    Args:
        request (SentimentRequest):
            The request object containing the model name and text for which the sentiment score needs to be calculated

    Returns:
        dict: A dictionary containing the sentiment score of the text in the range [-1, 1].
    """
    score = sentiment_score(text=request.text, model_name=request.model_name)
    return {"sentiment_score": score}


@app.post("/similarity_score")
async def similarity_score_route(request: SimilarityRequest):
    """
    Calculates the similarity score between two sentences using a specified model.

    Args:
        request (SimilarityRequest):
            The request object containing the model name and two sentences for which the similarity score needs to be calculated.

    Returns:
        dict: A dictionary containing the cosine similarity score between the two sentences.
    """
    score = similarity_score(
        sentence1=request.sentence1,
        sentence2=request.sentence2,
        model_name=request.model_name,
    )
    return {"similarity_score": score}
