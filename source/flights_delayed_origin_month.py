import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
import re
class Mapper(api.Mapper):
        def map(self, context):
                data = context.value.split(",")
                # data[14] => delay time
                # data[16] => origin airport code
                key_value = data[16] + " " + data[1]
                # If there is no data we assume that the flight is on time
                if ((data[15] == 'NA') or (data[15] == '') or (data[15] == ' ')):
                        temp = 0.0
                elif (re.search('[a-zA-Z]', data[15])):
                        temp = -1.0
                else:
                        temp = float(data[15])

                if (temp > 0):
                        context.emit(key_value, 1)
                else:
                        context.emit(key_value, 0)


class Reducer(api.Reducer):
        def reduce(self, context):
                #s = sum(context.values)
                numDelays = 0
                totalFlights = 0
                for value in context.values:
                        numDelays = numDelays + value
                        totalFlights = totalFlights + 1

                percent_delayed_flights = ((float(numDelays) * 100) /float( totalFlights))
                context.emit(context.key, percent_delayed_flights)

def __main__():
        pp.run_task(pp.Factory(Mapper, Reducer))
