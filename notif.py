# A EXECUTER SUR WINDOWS UNIQUEMENT
from win10toast import ToastNotifier

TITLE = "Notification"
MESSAGE = "Hello World!"
THREADED = True

if __name__ == "__main__":
    toaster = ToastNotifier()
    toaster.show_toast(TITLE, MESSAGE, threaded=THREADED)