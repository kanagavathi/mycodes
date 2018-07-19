import sys
print("This Script Converts text File into CSV")

data = {}
firstline = 0
headers=["datetime","Job_queue Size","Pipeline Staged","Pipeline Processing","Pipeline Success","Pipeline Failure","TaskInstance Submitted","TaskInstance Completed","Job_messenger__process_message_queue","ctm-jobhandler","ctm-ui","task"]

def read_text_file():
    try:
        filename = sys.argv[1]
        f = open(filename, mode='r', encoding='utf-8')
        for line in f.readlines():
            storyline = line
            line_partition(storyline)
    except:
        print("Error- Please provide correct Input file", sys.exc_info()[0])
        sys.exit()


def line_partition(line):
    if line.__contains__("====="):
        update_csv(data)
        data.clear()
    if line.__contains__("IST"):
        key = "datetime"
        value = line.partition("IST")[0]
        p = {key: value}
        data.update(p)
        # print(" Key %s = value %s " % (key, value))
    elif line.__contains__("Mongo") or line.__contains__("mongo"):
        return
    else:
        key = line.partition(":")[0]
        value = line.partition(":")[2]
        # print(" Key %s = value %s " %(key, value))
        p = {key: value}
        data.update(p)


def update_csv(data):
    try:
        out = open(sys.argv[2], 'a')
        global firstline
        global headers
        if firstline == 0:
            for header in headers:
                out.write(header + ",")
            firstline = 1
        for header in headers:
            value = data.get(header)
            if bool(value):
                value = value.partition("\n")
                out.write(value[0]+",")
            else:
                out.write(",")
        out.write("\n")
        out.close()
    except:
        print("Error- Please provide output file",)
        sys.exit()


if __name__ == '__main__':
    read_text_file()
