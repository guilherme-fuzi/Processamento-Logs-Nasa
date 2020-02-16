import pandas as pd
import re
import time


class Log:

    def __init__(self, path, regex, frame=''):
        self.path = path
        self.regex = regex
        self.frame = frame

    def load(self):
        list_df = []
        for chuck in pd.read_csv(self.path, sep=r'\n', engine='python', encoding='ISO-8859-1',
                                 header=None, chunksize=10 ** 5):
            list_df.append(chuck[:])
        self.frame = pd.concat(list_df)

        rows = []
        for d in self.frame[0]:
            rows.append([re.findall(x, d) for x in self.regex])

        self.frame = pd.DataFrame(rows[:], columns=['host', 'day', 'request', 'http', 'bytes'])

    def replace_to_zero(self, col, value_to_zero):
        self.frame[col] = self.frame[col].replace(value_to_zero, '0')

    def convert_str(self, columns):
        for c in columns:
            self.frame[c] = [str("".join(map(str, x))).strip() for x in self.frame[c]]


if __name__ == '__main__':
    start = round((time.monotonic() * 1000))

    regularEx = [r'^\S*', r'\[(.*?)\/', r'\"(.*?)\"', r'\s(\d+)\s', r'(\d+)(?!.*\d)']
    logJuly = Log('../resources/NASA_access_log_Jul95', regularEx)
    logJuly.load()
    logJuly.convert_str(logJuly.frame.columns)
    logJuly.replace_to_zero('bytes', '404')

    # 1
    print('\nQuantidade hosts unicos: ' + str(len(logJuly.frame['host'].value_counts())))

    # 2
    dataErr = logJuly.frame[logJuly.frame['http'].str.contains('404')].count()
    print('\nQuantidade de https 404: ' + str(dataErr['http']))

    # 3
    print('\nOs 5 URLs que mais causaram erro 404.')
    dataOrd = logJuly.frame[logJuly.frame['http'].str.contains('404')].groupby(['host']).count().sort_values(by='http')
    print(dataOrd['http'].tail())

    # 4
    dataByDay = logJuly.frame[logJuly.frame['http'].str.contains('404')].groupby(['day']).count()
    print(dataByDay['http'])

    # 5
    logJuly.frame.drop(logJuly.frame.tail(1).index, inplace=True)
    print(logJuly.frame['bytes'].astype('int64').sum())

    final = round(time.monotonic() * 1000)
    print('\nExecution time: ' + str((final - start) / 1000) + 's')

