import sys
import time

def update_progress_bar(param_progress):
    barLength = 20 # Modify this to change the length of the progress bar
    status = ""
    block = int(round(barLength * param_progress))
    text = "\rPercent: [{0}] {1}%".format( "#"* block + "-"* (barLength-block), int(param_progress*100))
    sys.stdout.write(text)
    sys.stdout.flush()


print("progress : 0->1")
for i in range(101):
    time.sleep(0.1)
    update_progress_bar(i/100.0)
