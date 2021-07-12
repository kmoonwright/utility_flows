from datetime import timedelta
from prefect import Flow
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

schedule = Schedule(clocks=[IntervalClock(timedelta(seconds=65))])

my_flow = Flow("Interval Test", schedule=schedule)

my_flow.register(project_name="experimental")