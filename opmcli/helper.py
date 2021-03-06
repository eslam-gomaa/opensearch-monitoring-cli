import math
import datetime

class Helper:
    def __init__(self):
        pass

    def bytes_to_kb_mb_gb(self,size_bytes):
        """
        Converts bytes to kb, mb, gb
        INPUT: size in bytes
        """
        if size_bytes == 0:
            return "0B"
        size_name = ("b", "kb", "mb", "gb", "tb", "pb", "eb", "zb", "yb")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    def kb_to_mb(self,size_kb):
        """
        Converts kilobytes to Megabytes
        INPUT: size in kilobytes
        """
        unit = "kb"
        if size_kb > 1000:
            unit = "mb"
            n = size_kb / 1000
            out = '%.2f' % n
            return f"{out} {unit}"
        out = '%.2f' % size_kb
        return f"{out} {unit}" 
    
    def sec_to_m_h(self, total_seconds):
        """
        Converts seconds to Minutes or Hours
        INPUT: seconds
        """
        out = '%.2f' % total_seconds
        unit = "sec"
        if (total_seconds > 60) and (total_seconds < 3600):
            n = total_seconds / 60
            out = '%.2f' % n
            unit = "min"
        elif total_seconds > 3600:
            n = total_seconds / 60 / 60
            out = '%.2f' % n
            unit = "hour/s"
        return f"{out} {unit}"


    def millisec_to_sec_m_h(self, milliseconds):
        """
        Converts milliseconds to seconds, Minutes or Hours
        INPUT: milliseconds
        """
        out = '%.2f' % milliseconds
        unit = "ms" # millis
        if milliseconds < 1000:
            return f"{out} {unit}"
        total_seconds = (milliseconds/1000)
        return self.sec_to_m_h(total_seconds)

    def millisec_to_d_h_m(self, milliseconds):
        """
        Converts milliseconds to format similar to 15d 08h 19m
        """
        return datetime.datetime.fromtimestamp(milliseconds/1000.0).strftime("%dd %Hh %Mm") # %Ss -> For seconds


    def percentage(self, part, whole):
        Percentage = 100 * float(part)/float(whole)
        return str(math.floor(Percentage)) + "%"
