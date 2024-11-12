%pip install googletrans==4.0.0-rc1 
 
from googletrans import Translator
 
translator = Translator()
 
def translate_text(text):
    try:
        translated_text = translator.translate(text, src='fr', dest='en').text
        return translated_text
    except Exception as e:
        print(f"Error translating from French: {e}")
        try:
            translated_text = translator.translate(text, src='it', dest='en').text
            return translated_text
        except Exception as e:
            print(f"Error translating from Italian: {e}")
            return text  # Return the original both fail
 
 
translated_text = translate_text("SANS")  # change value acording required
print(f"Translated text: {translated_text}")
 
 
 
 
 
 
OUTPUT:
Translated text: WITHOUT
 
i'm not sure this work for all but some case this work here for SANS
 
 