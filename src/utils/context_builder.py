from json import JSONDecodeError

import anthropic
import os
from dotenv import load_dotenv
import json
import logging
import pathlib

load_dotenv(pathlib.Path(__file__).parent.parent.parent / ".env")

logger = logging.getLogger("__name__")

load_dotenv()

def classify(user_input: str) -> dict:
    prompt = f"""
    You are a query classifier for a baseball stats application.
    Given a question, return a JSON object with these fields: 
      {{
        "query_type": "",
        "player_name": "",
        "team_name": "",
        "stat": "",
        "all_seasons": ""
      }}
    The keys are classified as follows:
        - query_type: player_batting, player_pitching, standings, leaderboard
        - player_name: The name of the player
        - team_name: null or the name of the team
        - stat: null or the stat
        - all_seasons: it's a boolean — true if the question asks for historical comparison, false if asking about current/recent performance.
        - question: {user_input}
        
    Return only raw JSON object, no explanation, no markdown.
    
    Example: How's Marcelo Mayer did in the 2025 season?
    output: 
        {{
            "query_type": "player_batting",
            "player_name": "Marcelo Mayer",
            "team_name": "Boston Red Sox",
            "stat": null,
            "all_seasons": false
        }}
    """
    client = anthropic.Anthropic()
    message = client.messages.create(
        model=os.getenv("AI_MODEL"),
        max_tokens=1024,
        messages=[{
        "role": "user",
        "content": prompt
        }]
    )
    try:
        return json.loads(message.content[0].text)
    except JSONDecodeError as e:
        logger.error(f"Error loading JSON File: {e}")
        return {}


print(classify("How is Marcelo Mayer Doing"))