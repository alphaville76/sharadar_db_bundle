"""Pipeline extensions for Sharadar fundamental data.

Provides customized Pipeline and ExecutionPlan classes that extend zipline
pipeline infrastructure for Sharadar fundamental data.
"""
from zipline.pipeline import Pipeline as ZiplinePipeline
from zipline.pipeline import Filter
from zipline.pipeline import ExecutionPlan as ZiplineExecutionPlan
from zipline.pipeline.domain import GENERIC, Domain


"""Pipeline execution module for Sharadar data processing.

This module extends the Zipline pipeline framework to provide custom execution
planning and pipeline compilation capabilities with support for named screens
and domain-specific configuration.
"""


class ExecutionPlan(ZiplineExecutionPlan):
    """Custom execution plan for Sharadar pipeline with screen name tracking.
    
    Extends Zipline's ExecutionPlan to include support for named screens,
    allowing the pipeline to reference and identify specific filtering logic
    used during execution.
    """

    def __init__(self, domain: Domain, terms, start_date, end_date, screen_name, min_extra_rows=0):
        """Initialize an ExecutionPlan with domain, terms, dates, and screen information.
        
        Args:
            domain (Domain): The pipeline domain (e.g., GENERIC, EQUITY).
            terms (dict): A dictionary of pipeline column/screen terms to be executed.
            start_date: The start date for the execution period.
            end_date: The end date for the execution period.
            screen_name (str): The name of the screen (filter) applied to this plan.
            min_extra_rows (int, optional): Minimum extra rows to compute for preprocessing.
                Defaults to 0.
        """
        super().__init__(domain, terms, start_date, end_date, min_extra_rows)
        self._screen_name = screen_name

    @property
    def screen_name(self):
        """Get the name of the screen applied to this execution plan.
        
        Returns:
            str: The screen name identifier.
        """
        return self._screen_name


class Pipeline(ZiplinePipeline):
    """Custom pipeline with support for named screens and execution planning.
    
    Extends Zipline's Pipeline to support flexible screen configuration with
    optional custom names, enabling better tracking and debugging of pipeline
    execution and filtering logic.
    """

    def __init__(self, columns=None, screen=None, domain=GENERIC):
        """Initialize a Pipeline with optional columns, screen filter, and domain.
        
        Args:
            columns (dict, optional): Dictionary mapping column names to Factor or other
                computable objects. Defaults to None (empty columns).
            screen (None, tuple, or Filter, optional): The filter to apply to rows.
                Can be:
                - None: No screen applied (uses default_screen during execution)
                - Filter: A single Filter instance (screen_name defaults to "screen_default")
                - tuple: A (str, Filter) pair where str is the screen name identifier
                
                Defaults to None.
            domain (Domain, optional): The pipeline domain. Defaults to GENERIC.
            
        Raises:
            TypeError: If screen is a tuple but not of the form (str, Filter).
            TypeError: If screen is not None, a Filter, or a (str, Filter) tuple.
        """
        if screen is None:
            super().__init__(columns, None, domain)
            self.screen_name = "screen_default"
        elif isinstance(screen, tuple):
            if not (isinstance(screen[0], str) and isinstance(screen[1], Filter)):
                raise TypeError("screen must be a (str, Filter) tuple")
            super().__init__(columns, screen[1], domain)
            self.screen_name = screen[0]
        elif isinstance(screen, Filter):
            super().__init__(columns, screen, domain)
            self.screen_name = "screen_default"
        else:
            raise TypeError("screen must be a (str, Filter) tuple or a Filter instance")

    def _prepare_graph_terms(self, default_screen):
        """Prepare pipeline terms for graph compilation with screen assignment.
        
        Combines columns with the screen filter, using the provided default_screen
        if no screen is currently set in the pipeline. Adds the screen to the terms
        under the key self.screen_name.
        
        Args:
            default_screen: The default screen to use if the pipeline has no screen set.
            
        Returns:
            dict: A copy of columns with the resolved screen added under self.screen_name key.
        """
        columns = self.columns.copy()
        screen = self.screen
        if screen is None:
            screen = default_screen

        columns[self.screen_name] = screen
        return columns

    def to_execution_plan(self, domain, default_screen, start_date, end_date):
        """Convert the Pipeline to an ExecutionPlan for compilation and execution.
        
        Creates an ExecutionPlan with the specified domain, date range, and graph terms.
        Validates that the pipeline's domain (if not GENERIC) matches the target domain
        to prevent domain mismatches during execution.
        
        Args:
            domain (Domain): The execution domain. Must match self._domain if self._domain
                is not GENERIC.
            default_screen: The default screen to use if no screen is set in the pipeline.
            start_date: The start date for the execution period.
            end_date: The end date for the execution period.
            
        Returns:
            ExecutionPlan: An ExecutionPlan ready for compilation with the resolved terms.
            
        Raises:
            AssertionError: If the pipeline's domain (when not GENERIC) differs from
                the provided domain, indicating a domain mismatch.
        """
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
