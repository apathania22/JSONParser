import csv
import datetime
import json
import pandas as pd
from itertools import groupby
from jinja2._compat import izip
import tempfile
import itertools as IT
import os
import shutil


def time_convert():
    df_csv = pd.read_csv('North Shore.csv')
    df = pd.DataFrame(df_csv)
    reader = csv.reader(df_csv)
    val0 = df.iloc[6:, 0]
    v0 = []
    v = []
    c = 0
    field = {}
    for i in val0:
        date_time_obj = datetime.datetime.strptime(i, '%m/%d/%Y %H:%M').timestamp()
        v0.append(int(date_time_obj))

    with open("Files/Time.json", "w")as t1:
        t1.write(json.dumps([{"timestamp": [str(Timestamp)]} for Timestamp in v0]))
    for row in reader:
        field[c] = row
        c = c + 1

    column = len(field)
    for _ in (number+1 for number in range(column-1)):
        del v[:]
        val1 = df.iloc[6:, _]
        for i in val1:
            v.append(float(i))
        uniquify(v)


def uniquify(v):
    def uniquify(path, sep=''):
        def name_sequence():
            count = IT.count()
            yield ''
            while True:
                yield '{s}{n:d}'.format(s=sep, n=next(count))

        orig = tempfile._name_sequence
        with tempfile._once_lock:
            tempfile._name_sequence = name_sequence()
            path = os.path.normpath(path)
            dirname, basename = os.path.split(path)
            filename, ext = os.path.splitext(basename)
            fd, filename = tempfile.mkstemp(dir=dirname, prefix=filename, suffix=ext)
            tempfile._name_sequence = orig

        makeFile(filename,v)
    (uniquify('Files/Value.json'))


def makeFile(filename,v):
    with open(filename, "w")as t2:
        t2.write(json.dumps([{"value": [val1]} for val1 in v]))
    uniTimeValName("Files/Time.json", filename)


def uniTimeValName(param, filename1):
    def uniquify(path, sep=''):
        def name_sequence():
            count = IT.count()
            yield ''
            while True:
                yield '{s}{n:d}'.format(s=sep, n=next(count))

        orig = tempfile._name_sequence
        with tempfile._once_lock:
            tempfile._name_sequence = name_sequence()
            path = os.path.normpath(path)
            dirname, basename = os.path.split(path)
            filename, ext = os.path.splitext(basename)
            fd, filename = tempfile.mkstemp(dir=dirname, prefix=filename, suffix=ext)
            tempfile._name_sequence = orig

        JoinTimeValue(param, filename1,filename)

    (uniquify('File/TimeValue.json'))


def JoinTimeValue(param, filename1,filename):
    with open(param) as f1:
        first_list = json.load(f1)
    with open(filename1) as f2:
        second_list = json.load(f2)

    for i, v in enumerate(second_list):
        first_list[i].update(v)

    with open(filename, "w")as f:
        f.write(json.dumps(first_list, indent=4))


def formatter(file1):
    f_out = open('Files/Out.csv', 'w')
    f = open(file1, 'r')

    for line in f:
             cols = line.split(',', 38)
             if len(cols) >= 38:
                 f_out.write( cols[0].strip() + ',' + cols[2].strip() + ',' + cols[3].strip() + "\n")
    f_out.close()
    f.close()


'''ef siteNameUpdate(param):
    r = csv.reader(open(param))
    lines = list(r)
    lines[4][0] = 'North Shore'
    writer = csv.writer(open('Files/Out.csv', 'w'))
    writer.writerows(lines)'''


def add_DeviceName(param):
    df = pd.read_csv(param)
    df['device'] = "2160 LaserFlow Module"
    df.to_csv('Files/Out.csv')


def update_CSV(p):
    with open(p,"r") as source:
        rdr= csv.reader( source )
        with open("Files/Out1.csv","w") as result:
            wtr= csv.writer( result )
            for r in rdr:
                wtr.writerow( (r[1], r[2], r[3], r[4]) )

def dataJoin(fileList, i):
    with open(os.path.join('File/' + fileList[i]), 'r') as fs:
        content = json.load(fs)
    return content


def joinFinal(infile1, file):
    fileList = file

    with open('Files/Out1.csv', 'r') as infile1:
        r = csv.DictReader(infile1)
        data = [dict(d) for d in r]

    groups = []
    i = 0
    for k, g, in (groupby(data, lambda r: (r['Site Name'], r['Label'], r['Units'], r['device']))):
        groups.append({
            "Type": "UPDATE",
            "resource": {
                "site ": k[0],
                "name": k[1],
                "unit": k[2],
                "device": k[3],
                "data": dataJoin(fileList, i),
                "type": "QUANTITY",
                "status": "ACTIVE"

            }
        })
        i += 1

    with open("Result.json", "w")as w:
        w.write(json.dumps(groups[:20], indent=4))



if __name__ == '__main__':
    os.makedirs('Files', exist_ok=True)
    os.makedirs('File', exist_ok=True)
    time_convert()
    file_list = os.listdir(r"File")

    a = izip(*csv.reader(open("North Shore.csv", "r")))
    csv.writer(open("Files/ColtoRow.csv", "w")).writerows(a)
    formatter("Files/ColtoRow.csv")
   #siteNameUpdate("Files/Out.csv")
    add_DeviceName("Files/Out.csv")
    update_CSV("Files/Out.csv")
    joinFinal("Files/Out1.csv", file_list)
    # shutil.rmtree("Files")
    # shutil.rmtree("File")

