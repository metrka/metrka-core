"""Built-in quality checks.

Every check is a plain function that returns an :class:`Outcome`. Positional
arguments are what is checked (a file, a table); keyword arguments are settings
and are recorded as the check's params.

``files``    the landed source file (pre_bronze)
``outputs``  files the pipeline wrote (post_bronze, post_silver)
``tables``   a Silver table and its columns (pre_silver, post_silver)
"""
