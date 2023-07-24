import googletrans
from googletrans import Translator

translator = Translator()
text ='None'
result = translator.translate(text, src='en', dest='vi')
print(result.text)


# 'en': 'english'
# 'vi': 'vietnamese'
