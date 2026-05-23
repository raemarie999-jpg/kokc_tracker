# Gunicorn config — starts background fetch thread after worker fork
def post_fork(server, worker):
    from app import start_background
    start_background()
