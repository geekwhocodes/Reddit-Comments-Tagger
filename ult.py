import re
import contactions
import badwords

contractions = contactions.get()
bad_words = badwords.get()

def expandShort(sent):
    for word in sent.split():
        if word.lower() in contractions:
            sent = sent.replace(word, contractions[word.lower()])
    return sent

def cleanText(sent):
    sent = str(sent)
    sent = sent.replace("\\n","")            
    sent = sent.replace("\\xa0","") #magic space lol
    sent = sent.replace("\\xc2","") #space
    sent = sent.replace("0xb0", "")
    sent = re.sub(r"(@[A-Za-z]+)|([\t])", "",sent)
    sent = expandShort(sent.strip().lower())
    sent = re.sub(r'[^\w]', ' ', sent)
    sent = re.sub(r"(@[A-Za-z]+)|([^A-Za-z \t])", " ", sent)
    ws = [w for w in sent.strip().split(' ') if w is not ''] # remove double space
    return " ".join(ws)

def isAbusive(id,sent):
    sent = cleanText(sent)
    w = set(sent.lower().strip().split(' '))
    b = set(bad_words)
    result = w.intersection(b)
    if len(result) > 0:
        return id, True
    else:
        return id, False