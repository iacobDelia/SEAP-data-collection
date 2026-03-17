from google import genai
from google.genai import types
import json
import os
from dotenv import load_dotenv
import sys
load_dotenv()


API_KEY = os.getenv("LLM_API_KEY")
client = genai.Client(api_key=API_KEY)

SYSTEM_PROMPT = """
Ești un expert în achiziții publice pe IT (CPV 72000000).
Analizează următorul text dintr-un Caiet de Sarcini și extrage datele în format JSON strict:
      
Câmpuri cerute:
1. nr_module_software: componente funcționale sau module software distincte (integer)
2. nr_experti_cheie (numărul total de persoane, nu de roluri) (integer)
3. durata_proiect_luni (implementare) (integer)
"""

def analyze_document(file_path):
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return None

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            document_text = file.read()

        response = client.models.generate_content(
            model="models/gemini-2.5-flash",
            contents=document_text,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                response_mime_type="application/json",
            ),
        )

        return json.loads(response.text)

    except Exception as e:
        print(f"exception: {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("no file specified :(")
    else:
        file_to_analyze = sys.argv[1]
        result = analyze_document(file_to_analyze)
        if result:
            print(json.dumps(result, indent=4, ensure_ascii=False))