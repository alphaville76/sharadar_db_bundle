from zipline.pipeline import Pipeline as ZiplinePipeline
from zipline.pipeline import Filter
from zipline.pipeline import ExecutionPlan as ZiplineExecutionPlan
from zipline.pipeline.domain import GENERIC, Domain
from collections import namedtuple

from zipline.utils.input_validation import expect_types, optional

class ExecutionPlan(ZiplineExecutionPlan):

    def __init__(self, domain, terms, start_date, end_date, screen_name, min_extra_rows=0):
        super().__init__(domain, terms, start_date, end_date, min_extra_rows)
        self._screen_name = screen_name

    @property
    def screen_name(self):
        return self._screen_name


class Pipeline(ZiplinePipeline):

    @expect_types(columns=optional(dict), screen=optional(tuple), domain=Domain)
    def __init__(self, columns=None, screen=None, domain=GENERIC):
        if not (isinstance(screen[0], str) and isinstance(screen[1], Filter)):
            raise TypeError("screen must be a (str, Filter) tuple")
        super().__init__(columns, screen[1], domain)
        self.screen_name = "screen_default" if screen is None else screen[0]

    def _prepare_graph_terms(self, default_screen):
        columns = self.columns.copy()
        screen = self.screen
        if screen is None:
            screen = default_screen

        columns[self.screen_name] = screen
        return columns

    def to_execution_plan(self, domain, default_screen, start_date, end_date):
        if self._domain is not GENERIC and self._domain is not domain:
            raise AssertionError(
                "Attempted to compile Pipeline with domain {} to execution "
                "plan with different domain {}.".format(self._domain, domain)
            )

        return ExecutionPlan(
            domain=domain,
            terms=self._prepare_graph_terms(default_screen),
            start_date=start_date,
            end_date=end_date,
            screen_name=self.screen_name
        )


