from decouple import config
import xml.etree.ElementTree as ET
import pathlib
import re




def divide_chunks(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]

def body_score(xml):
    words = re.findall(r'(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))', xml.attrib['Body'])
    score = int(xml.attrib['Score'])
    return {
        'id': xml.attrib['Id'],
        'len_words': len(words),
        'score': score,
        'relation': len(words) / (score,1)[score == 0]
    }



def mapped(data_chunk):
    mappedOne = list(map(body_score, data_chunk))
    return mappedOne
    

if '__main__' == __name__:
    #path in config('dataset_path')
    path = str(pathlib.Path().absolute()) +'/..' + config('dataset_path') + 'posts.xml'
    tree_xml = ET.parse(path)
    data_chunks = divide_chunks(tree_xml.getroot(), 100)
    mapped = list(map(mapped, data_chunks))
    print(mapped)