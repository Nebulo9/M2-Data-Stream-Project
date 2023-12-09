# A EXECUTER SUR WINDOWS UNIQUEMENT
from win10toast import ToastNotifier
import time

def send_notification(title, message,duration=3,icon_path=None,threaded=False,cooldown=None):
    toaster = ToastNotifier()
    toaster.show_toast(title, message,icon_path=icon_path,duration=duration,threaded=threaded)
    if cooldown:
        time.sleep(duration + cooldown)
    else:
        while toaster.notification_active():
            time.sleep(0.1)

if __name__ == "__main__":
    send_notification("test","test")
    send_notification("test2","test2")