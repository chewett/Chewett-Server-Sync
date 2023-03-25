import os
import tarfile
from CommandRunner import run_command


class Compressor:

    def __init__(self, method):
        if method not in ["python", "gzip", "pigz"]:
            raise Exception("Unsupported compression method")

        self.method = method


    def compress(self, location, outputfile):
        real_output_file = outputfile

        if self.method in ["python", "gzip", "pigz"]:
            real_output_file += ".tgz"

        print("Compressing " + location + " to " + real_output_file)

        if self.method == "python":
            self._compress_python(location, real_output_file)
        if self.method == "gzip":
            self._compress_gzip(location, real_output_file)

        else:
            raise Exception("Unsupported compression method")

        return real_output_file


    def _compress_python(self, location, outputfile):
        print("Using native python for compression...")
        with tarfile.open(outputfile, "w:gz") as tar:
            for name in os.listdir(location):
                tar.add(os.path.join(location, name), name)

    def _compress_gzip(self, location, outputfile):
        print("Using OS tar with gzip for compression...")
        loc_folder = os.path.dirname(location)
        loc_name = os.path.basename(location)
        run_command("tar zcf " + outputfile + " -C  " + loc_folder + " " + loc_name)



