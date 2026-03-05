"""
Copyright (C) 2025, CEA

This program is free software; you can redistribute it and/or modify
it under the terms of the Creative Commons Attribution-NonCommercial-ShareAlike 4.0
International License.

You should have received a copy of the license along with this
program. If not, see <https://creativecommons.org/licenses/by-nc-sa/4.0/>.
"""

import os


class TempPcapHandler:

    def __init__(self, path: str, rotate_every_t_seconds: float) -> None:

        self.temp_pcap_path = path
        self.rotate_every_t_seconds = rotate_every_t_seconds

        self.temp_pcap_files_ready: list[str]

        if not os.path.exists(self.temp_pcap_path):
            os.makedirs(self.temp_pcap_path)


    def clean_pcap_files(self):
        for filename in os.listdir(self.temp_pcap_path):
            if filename.endswith(".pcap"):
                os.remove(os.path.join(self.temp_pcap_path, filename))


    def _update_pcap_files_ready_list(self):
        pcap_files = [
            os.path.join(self.temp_pcap_path, filename) for filename in os.listdir(self.temp_pcap_path)
            if filename.endswith(".pcap")

        ]
        
        if len(pcap_files) <= 1:
            self.temp_pcap_files_ready = []
            return

        
        # NOTE: The solution with os.path.getmtime does not work since tcpdump writing on the file some reasons doesn't update the file's mtime
        self.temp_pcap_files_ready = sorted(pcap_files, key=lambda filepath: os.path.getmtime(filepath))[:-1]



    def get_next_pcap_file(self) -> str | None:
        self._update_pcap_files_ready_list()

        if len(self.temp_pcap_files_ready) == 0:
            return None
        
        return self.temp_pcap_files_ready.pop()

    def remove_pcap_file(self, pcap_file: str):
        if self._file_belongs_to_dir(pcap_file, self.temp_pcap_path):
            os.remove(pcap_file)
            return
        
        raise ValueError("Input pcap_file does not belong to directory")

    @staticmethod
    def _file_belongs_to_dir(file_path: str, dir_path: str):
        try:
            common = os.path.commonpath([file_path, dir_path])
            return common == dir_path
        except Exception:
            return False
