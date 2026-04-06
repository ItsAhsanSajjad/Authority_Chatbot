import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")
print(f"Key loaded: {api_key[:5]}...{api_key[-4:] if api_key else 'None'}")

client = OpenAI(api_key=api_key)

try:
    print("Testing gpt-4o-mini...")
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "Hello! Are you working?"}]
    )
    print("Success!")
    print("Response:", resp.choices[0].message.content)
except Exception as e:
    print(f"Error: {e}")
