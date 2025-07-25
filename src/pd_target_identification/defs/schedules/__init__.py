"""Pipeline schedules."""

from dagster import schedule, ScheduleDefinition

@schedule(cron_schedule="0 0 * * 0", job_name="target_discovery_job")  
def weekly_target_discovery_schedule():
    """Run target discovery pipeline weekly."""
    return {}

# Create a pipeline_schedules object that can be imported
class PipelineSchedules:
    weekly_target_discovery_schedule = weekly_target_discovery_schedule

pipeline_schedules = PipelineSchedules()

__all__ = ["pipeline_schedules"]
