"""
This file used to extract and format the log files in defined format

input file: fixture.log
output file: output.csv

Execution:

> python SFAnalysisProject.py < /path/to/input/file .log> </path/to/output/file.csv>

"""

import sys
import datetime

# need to update for interval range
ONE_MINUTE = 1
TEN_MINUTE = 10
ONE_HOUR = 60


class FormatLog:
    def __init__(self, log_file_path, output_file_path, range_value):
        """

        :param log_file_path: the file path of input file
        :param output_file_path: file path of output file
        :param range_value: Time Period for aggregation (1 Min/10 Mins/ 1Hr)
        """
        self.log_file_path = log_file_path
        self.output_file_path = output_file_path
        self.range_value = range_value

    def extract_data(self):
        """
        This function used to extract from fixture.log file
        """
        log_data = {}
        log_file = open(self.log_file_path, 'r')
        # Read the log file by splitting the data with \n as delimiter
        min_time_stamp = None
        max_time_stamp = None
        for log in log_file.read().split("\n"):
            if log:
                timestamp = log.split(' ')[0][:16]  # Getting the timestamp value by splitting the string and get the first value
                host = log.split("host=")[1].split(" ")[0]  # Get the host value by splitting the log with host key
                service = int(log.split("service=")[1].split("ms")[0])  # Get the service time for the request

                # Getting the current datetime object
                current_timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M")

                if not min_time_stamp:
                    min_time_stamp = current_timestamp
                    max_time_stamp = min_time_stamp + datetime.timedelta(minutes=self.range_value)
                # if current timestamp greater than the range value then the range is incremented by range value
                while current_timestamp >= max_time_stamp:
                    min_time_stamp = max_time_stamp
                    max_time_stamp = max_time_stamp + datetime.timedelta(minutes=self.range_value)

                key_value = "{} - {}".format(min_time_stamp.strftime("%Y-%m-%dT%H:%M"), max_time_stamp.strftime(
                    "%Y-%m-%dT%H:%M"))  # Setting the range as the key value pair start_time to end _time
                if key_value in log_data:
                    if host in log_data[key_value]:
                        log_data[key_value][host]['service'].append(service)
                    else:
                        log_data[key_value][host] = {'service': [service], 'timestamp': timestamp}
                else:
                    log_data[key_value] = {host: {'service': [service], 'timestamp': timestamp}}
        return log_data

    def write_output_data(self, log_data):
        """
        Format the .log file data and store as .csv file
        """
        final_output = []
        time_stamps = list(log_data.keys())
        time_stamps.sort()

        for key_value in time_stamps:
            hosts = list(log_data[key_value].keys())
            hosts.sort()
            for host in hosts:
                service_time = log_data[key_value][host]['service']
                service_time.sort()
                final_output.append(
                    "{}:00,{},{},{},{},{}".format(log_data[key_value][host]['timestamp'], host, len(service_time),
                                                  sum(service_time), service_time[0], service_time[-1]))
        final_output = "\n".join(final_output)
        f = open(self.output_file_path, 'w')
        f.write(final_output)


if __name__ == "__main__":
    log_file_path = sys.argv[1]  # input file path argument
    output_file_path = sys.argv[2]  # output file path argument
    fl = FormatLog(log_file_path, output_file_path, ONE_MINUTE)  # Summarize time period 1min, 10minutes & 1 hour
    log_data = fl.extract_data()  # calling extract_data() function
    fl.write_output_data(log_data)  # calling write_output_data() function


