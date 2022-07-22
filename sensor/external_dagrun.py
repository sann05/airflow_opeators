from datetime import datetime, timedelta

from airflow import AirflowException
from airflow.models import DagRun, DagBag
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.db import provide_session
from airflow.utils.state import State
from croniter import croniter


class LastExternalDagRunStatusSensor(BaseSensorOperator):
    """
    The sensor checks that status of the last started DagRun in allowed states
    """
    template_fields = ['external_dag_id']
    ui_color = '#19647e'

    def __init__(self,
                 external_dag_id,
                 allowed_states=None,
                 check_existence=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]

        self.external_dag_id = external_dag_id
        self.check_existence = check_existence

    @provide_session
    def poke(self, context, session=None):
        self.log.info(
            'Poking for %s states in %s ... ',
            self.external_dag_id,
            str(self.allowed_states)
        )

        DR = DagRun
        last_dag_run = session.query(DR).filter(
            DR.dag_id == self.external_dag_id
        ).order_by(DR.execution_date.desc()).first()

        return last_dag_run.state in self.allowed_states


class ClosestExternalTaskSensor(ExternalTaskSensor):
    """
    Sensor finds closes external execution date before or after the sensor
    execution date
    """

    def _get_external_latest_execution_date(self, execution_date):
        external_dag = DagBag().get_dag(self.external_dag_id)
        if not isinstance(external_dag.schedule_interval, str):
            raise AirflowException(
                "Only possible to work with str schedule_interval, not "
                f"{type(external_dag.schedule_interval)} for"
                f" {external_dag.schedule_interval}"
            )
        itr = croniter(external_dag.schedule_interval, execution_date)
        dt = itr.get_next(datetime) if self.is_following else itr.get_prev(datetime)
        self.log.info("The closest "
                      + ('following' if self.is_following else 'preceding')
                      + f" execution_date for {self.external_dag_id} is {dt}")
        return dt

    def __init__(self, *args, **kwargs) -> None:
        """

        :param is_following: if True find closest following else find closest
        preceding. Default False
        """
        # Mock value of execution_delta to avoid errors during initialization
        # of the parent sensor
        kwargs["execution_delta"] = timedelta(days=1)
        self.is_following = bool(kwargs.get("is_following"))
        super().__init__(*args, **kwargs)

    @provide_session
    def poke(self, context, session=None):
        execution_date = context["execution_date"]
        self.execution_delta = execution_date - self._get_external_latest_execution_date(
            execution_date)
        return super().poke(context, session)
