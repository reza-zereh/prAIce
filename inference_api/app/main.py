from fastapi import FastAPI

from .libs.sentiment import sentiment_score
from .libs.summarizer import summarize
from .schema import SentimentRequest, SummarizationRequest

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
