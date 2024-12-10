"""
Below is both Scala and Python code for calculating the data 30 day ago from today and format or manipulate the result, but they use different libraries and methods to achieve it. The Scala code focuses more on formatting and working with Java's LocalDate API, while the Python code relies on Python's datetime and timedelta classes, although the part calculating 30 days ago is commented out.

SCALA CODE
1. import java.time.LocalDateTime and import java.time.format.DateTimeFormatter
These are imports from Java's java.time package, which provides classes for handling date and time.
LocalDateTime: This class represents a date and time without a time zone.
DateTimeFormatter: This class is used to format and parse dates and times.

2. var timeNow = LocalDate.now()
This line gets the current date using the LocalDate class (without the time part).
timeNow will contain today's date, for example, "2024-12-10".

3. var timeNowModified = DateTimeFormatter.ofPattern("yyyyMMdd")
DateTimeFormatter.ofPattern("yyyyMMdd") creates a formatter that will format the date into a string with the pattern yyyyMMdd, which means:
yyyy: Year with four digits (e.g., 2024).
MM: Month with two digits (e.g., 12).
dd: Day of the month with two digits (e.g., 10).
timeNowModified is now a DateTimeFormatter that will convert LocalDate into a string in the yyyyMMdd format.

4. var thirtyDaysAgo = timeNowModified.format(timeNow.minusDays(30))
timeNow.minusDays(30) subtracts 30 days from the current date (timeNow), effectively giving the date 30 days ago.
timeNowModified.format(...) then formats this new date (30 days ago) into a string using the previously defined timeNowModified formatter (yyyyMMdd).
So, thirtyDaysAgo will hold the date 30 days ago formatted as a string like "20241110" (assuming today is "2024-12-10").

PYTHON CODE
1. from datetime import datetime, timedelta
This imports the necessary classes from Python's datetime module:
datetime: Represents a date and time object.
timedelta: Represents the difference between two dates or times (e.g., adding or subtracting days).

2. timeNow = datetime.now()
datetime.now() returns the current date and time as a datetime object. It will include the current year, month, day, hour, minute, second, and microsecond.
timeNow will hold something like 2024-12-10 15:30:45.123456.

3. timeNowModified = datetime.strptime(str(timeNow), '%Y-%m-%d')
str(timeNow) converts the datetime object into a string (e.g., '2024-12-10 15:30:45.123456').
datetime.strptime() parses the string representation of timeNow and creates a new datetime object with just the date portion, using the format '%Y-%m-%d'.
%Y-%m-%d: This format extracts only the year, month, and day (ignores time).
The result is a datetime object representing just the date, like 2024-12-10.

4. # thirtyDaysAgo = timeNow + timedelta(days = int(30))
This line is commented out, but it would compute the date 30 days ago if it were active.
timedelta(days=30) creates a time delta object representing a span of 30 days.
timeNow + timedelta(days=30) would add 30 days to the timeNow object. (To calculate 30 days ago, it would need to subtract the timedelta instead.)
"""

%scala

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

var timeNow = LocalDate.now()
var timeNowModified = DateTimeFormatter.ofPattern("yyyyMMdd")
var thirtyDaysAgo = timeNowModified.format(timeNow.minusDays(30))

%python

from datetime import datetime, timedelta
import time

timeNow = datetime.now()
timeNowModified = datetime.strptime(str(timeNow), '%Y-%m-%d')
# thirtyDaysAgo = timeNow + timedelta(days = int(30))
