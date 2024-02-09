from googletrans import Translator

t = Translator()
print(t.translate("Hello", dest="or").text)