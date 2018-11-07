import urllib.request
import shutil

with urllib.request.urlopen('http://central.maven.org/maven2/junit/junit/4.12/junit-4.12.jar') as response, open('junit-4.12.jar', 'junit.jar') as out_file:
    shutil.copyfileobj(response, out_file)