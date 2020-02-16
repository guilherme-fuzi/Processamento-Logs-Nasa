import re
import time


def row_clean(row):
    row_clear = []
    for r in row:
        row_clear.append("".join(x for x in r))

    return row_clear


def row_to_dataframe(df, row):
    if len(row) == 5:
        df.append(row)
    elif len(row) == 4:
        row.append('0')
        df.append(row)
    elif len(row) == 1:
        pass

    return row


def dict_table(dictionary):
    for k, v in dictionary.items():
        print(str(k) + ' -> ' + str(v))


class Log:

    def __init__(self, path, regex, frame=[], columns=None):
        if columns is None:
            columns = {'host': 0, 'day': 1, 'requisition': 2, 'http': 3, 'bytes': 4}
        self.path = path
        self.regex = regex
        self.frame = frame
        self.columns = columns

    def load(self):
        arq = open(self.path, encoding='ISO-8859-1')
        for row in arq:
            row = re.findall(self.regex, row)
            clean_row = row_clean(row)
            row_to_dataframe(self.frame, clean_row)

    def group_by(self, column):
        index_column = self.columns.get(column)
        result = {}
        for row in self.frame:
            if row[index_column] not in result:
                result[row[index_column]] = 0
            result[row[index_column]] += 1

        return result

    def value_count(self, column, value):
        index_column = self.columns.get(column)
        result = {value: 0}
        for row in self.frame:
            if value in str(row[index_column]):
                result[value] += 1

        return result

    def filter_col(self, column, name_column_value, value):
        index_column = self.columns.get(column)
        index_column_value = self.columns.get(name_column_value)
        result = {}
        for row in self.frame:
            if value == str(row[index_column_value]):
                if str(row[index_column]) not in result:
                    result[str(row[index_column])] = 0
                result[str(row[index_column])] += 1

        return result

    def column_sum(self, column):
        index_column = self.columns.get(column)
        sum = 0
        for row in self.frame:
            try:
                sum += int(row[index_column])
            except TypeError:
                print('Erro na neste registro: ' + str(row[index_column]))

        return sum


if __name__ == '__main__':
    start = round((time.monotonic() * 1000))

    regularExp = r'(^\S*)|\[(.*?)\/|\"(.*?)\"|\s(\d+)\s|(\d+)(?!.*\d)'
    pathJuly = '../resources/NASA_access_log_Jul95'

    logJuly = Log(pathJuly, regularExp)
    logJuly.load()

    # 1
    print('\nNúmero de hosts únicos: ' + str(len(logJuly.group_by('host'))))

    # 2
    print('\nQuantidade de erros 404: ' + str(logJuly.value_count('http', '404')))

    # 3
    print('\nOs 5 URLs que mais causaram erro 404: ')
    dict_host = logJuly.filter_col('host', 'http', '404')
    for w in sorted(dict_host, key=dict_host.get, reverse=True)[:5]:
        print(' -  ' + w + ' ::: ' + str(dict_host.get(w)))

    # 4
    print('\nQuantidade de erros 404 por dia: ')
    aux = logJuly.filter_col('day', 'http', '404')
    dict_day = {int(k): int(v) for k, v in aux.items()}
    for w in sorted(dict_day):
        print('Dia: ' + str(w) + ' -> ' + str(dict_day.get(w)))

    # 5
    print('\nTotal de bytes retornados: ' + str(logJuly.column_sum('bytes')))

    final = round(time.monotonic() * 1000)
    print('\nExecution time: ' + str((final - start) / 1000) + 's')
