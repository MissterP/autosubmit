"""CPMIP metrics evaluation module."""


class CPMIPMetrics:
    """Evaluates CPMIP performance metrics against configured thresholds."""

    @staticmethod
    def _fetch_metrics(job):
        """Fetch CPMIP metrics for a job (source to be determined).
        
        This is a placeholder stub. Future implementation will:
        - Query UserMetricRepository or external metrics source
        - Return dict like {"SYPD": 5.2, "LATENCY": 9.1, ...}
        
        :param job: Job object with metrics
        :return: Dict of metric_name -> value
        """
        # TODO: Implement actual metric fetching
        return {}

    @staticmethod
    def evaluate(job, thresholds):
        """Evaluate job metrics against thresholds and return violations.
        
        :param job: Job object
        :param thresholds: Dict of {metric_name: {THRESHOLD, COMPARISON, %_ACCEPTED_ERROR}}
        :return: Dict of {metric_name: {threshold, accepted_error, real_value}} for violations only
        """
        if not thresholds:
            return {}
        
        metrics = CPMIPMetrics._fetch_metrics(job)
        violations = {}
        
        for metric_name, threshold_config in thresholds.items():
            if metric_name not in metrics:
                continue
            
            real_value = metrics[metric_name]
            threshold = threshold_config["THRESHOLD"]
            comparison = threshold_config["COMPARISON"]
            accepted_error = threshold_config.get("%_ACCEPTED_ERROR", 0)
            
            tolerance_factor = accepted_error / 100.0
            
            is_violation = False
            
            if comparison == "greater_than":
                # Metric must be >= threshold * (1 - tolerance%)
                # Violation if real_value < threshold * (1 - tolerance%)
                lower_bound = threshold * (1 - tolerance_factor)
                if real_value < lower_bound:
                    is_violation = True
            
            elif comparison == "less_than":
                # Metric must be <= threshold * (1 + tolerance%)
                # Violation if real_value > threshold * (1 + tolerance%)
                upper_bound = threshold * (1 + tolerance_factor)
                if real_value > upper_bound:
                    is_violation = True
            
            if is_violation:
                violations[metric_name] = {
                    "threshold": threshold,
                    "accepted_error": accepted_error,
                    "real_value": real_value,
                }
        
        return violations