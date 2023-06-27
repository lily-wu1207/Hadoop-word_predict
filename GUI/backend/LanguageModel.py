from typing import NoReturn, List
from pathlib import Path
import json
from collections import Counter

class LanguageModel(object):

    def __init__(self, model_path: Path):
        super().__init__()
        self.__predict_dict = {}
        self.__load(model_path)

    def __load(self, path: Path) -> NoReturn:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    line_list = line.strip('\n').split('\t', 2)
                    prefix = line_list[0]
                    suggest_dict = json.loads(line_list[1])
                    self.__predict_dict[prefix] = suggest_dict
                except Exception as err:
                    print(err)
                    pass

    def getSuggestList(self, prefix: str) -> List[str]:
        bi_gram = prefix[2:3]
        tri_gram = prefix[1:3]
        qua_gram = prefix
        alpha = 0.3
        bi_dict = self.__predict_dict.get(bi_gram, {})
        bi_dict.update((x, y * alpha * alpha) for x, y in bi_dict.items())
        tri_dict = self.__predict_dict.get(tri_gram, {})
        tri_dict.update((x, y * alpha) for x, y in tri_dict.items())
        qua_dict = self.__predict_dict.get(qua_gram, {})

        X, Y, Z = Counter(bi_dict), Counter(tri_dict), Counter(qua_dict)
        pre_dict = dict(X + Y + Z)
        print(pre_dict)
        suggest_list = [k for k,_ in sorted(pre_dict.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)]
        predict = suggest_list[:20] if len(suggest_list) > 20 else suggest_list
        return predict




if __name__ == '__main__':
    lm = LanguageModel(Path("./output/small_sample"))
    print(lm.getSuggestList("段经"))
