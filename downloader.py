import datetime
import subprocess
import time
import signal
import os
import shutil
import glob
import logging
import slack
import threading
import argparse

from logging.handlers import RotatingFileHandler

VIDEO_TIME = 30
URL = "https://56cdb389b57ec.streamlock.net:1935/nps/faithful.stream/chunklist_w940365989.m3u8"
ORIGINAL_DIR = os.getcwd()


class DataCollector:
    def __init__(self, notifier):
        self.notifier = notifier

    def download_chunk(self):
        logging.info('Beginning video download')

        start_time = str(datetime.datetime.now()).split('.')[0]
        download_process = subprocess.Popen(["youtube-dl", "--no-part", "-f", "mp4", "-o", f"{start_time}.mp4", URL],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        time.sleep(VIDEO_TIME)
        download_process.send_signal(signal.SIGINT)
        output, error = download_process.communicate()

        error_msg = error.decode('utf-8')

        if download_process.returncode != 0 and 'ERROR: Interrupted by user' not in error_msg:
            self.notifier.handle_error('Could not download video from source', error)
        else:
            logging.info('Video downloaded successfully')

    def process_videos(self):
        files = list(glob.glob("*.mp4"))

        if len(files) == 0:
            logging.info('No videos to process')
        else:
            logging.debug('Found following videos to process')
            logging.debug(str(files))

        for file in files:
            logging.info(f'Processing file {file}')
            ffmpeg_proces = subprocess.Popen(['ffmpeg', '-i', file, 'thumb%04d.jpg', '-hide_banner'],
                                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = ffmpeg_proces.communicate()

            if ffmpeg_proces.returncode != 0:
                self.notifier.handle_error('Could not split video into frames using FFMPEG', error)
                os.remove(file)
            else:
                ffmpeg_proces.wait(timeout=180)
                logging.debug('Successfully split video into frames')
                start_time_str = file.split('.')[0]
                start_time = datetime.datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
                end_time = start_time + datetime.timedelta(seconds=DataCollector.get_video_length(file))
                self._move_images(start_time, end_time)
                logging.debug('Successfully moved and renamed frames')
                os.remove(file)
                logging.info('Successfully processed file')

    def _move_images(self, start_time, end_time):
        images = sorted(list(glob.glob("*.jpg")))

        if len(images) == 0:
            self.notifier.handle_error('_move_images called but no images were found')
            return

        current_time = start_time
        time_range = end_time - current_time
        seconds_interval = time_range.seconds / len(images)

        path_to_frames = os.path.join(ORIGINAL_DIR, 'frames')

        for image in images:
            new_name = current_time.strftime('%Y-%m-%d %H:%M:%S')
            shutil.move(os.path.join(ORIGINAL_DIR, image), os.path.join(path_to_frames, ''.join([new_name, '.jpg'])))
            current_time += datetime.timedelta(seconds=seconds_interval)

    @staticmethod
    def get_video_length(filename):
        result = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                                 "format=duration", "-of",
                                 "default=noprint_wrappers=1:nokey=1", filename],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        return float(result.stdout)


class Notifier:
    def __init__(self, slack_token, message_interval, max_gigs, size_warn_percent):
        self.time_last_sent = None
        self.message_interval = message_interval * 60
        self.slack_token = slack_token
        self.client = slack.WebClient(token=self.slack_token)
        self.max_gigs = max_gigs
        self.size_warn_percent = size_warn_percent

    def handle_error(self, error_msg, error_obj=None):
        logging.error(error_msg)

        if error_obj is not None:
            logging.error(error_obj.decode('utf-8'))

        self.send_message(error_msg)
        log_path = os.path.join(ORIGINAL_DIR, 'log')
        log_tail = subprocess.check_output(["tail", log_path]).decode('utf-8')
        self.send_message(log_tail)

    def send_message(self, message):
        current_time = datetime.datetime.now()

        if self.time_last_sent is None or (current_time - self.time_last_sent).seconds > self.message_interval:
            self.time_last_sent = current_time
            self.client.chat_postMessage(channel='app-notifications', text=message)

    def monitor_size(self):
        cur_size = self._get_dir_size('./frames')
        if cur_size >= self.max_gigs * self.size_warn_percent:
            self.send_message(f'WARNING: {cur_size}GB USED OF {self.max_gigs}GB')

    def _get_dir_size(self, dir):
        nbytes = sum(d.stat().st_size for d in os.scandir(dir) if d.is_file())
        return nbytes * 1e-9


def setup_directory(dir):
    if not os.path.isdir(dir):
        os.mkdir(dir)


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Continuously extracts frames from Old Faithful stream')
    parser.add_argument('slack_token', help='Slack key')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            RotatingFileHandler(os.path.join(os.path.split(__file__)[0], 'log'), maxBytes=5 * 1024 * 1024,
                                backupCount=1, delay=False),
            logging.StreamHandler()
        ]
    )

    setup_directory('frames')

    notifier = Notifier(slack_token=args.slack_token, message_interval=60, max_gigs=64, size_warn_percent=.5)
    data_collector = DataCollector(notifier)
    notifier.send_message('test')

    while True:
        data_collector.download_chunk()
        run_threaded(data_collector.process_videos)
        run_threaded(notifier.monitor_size)

