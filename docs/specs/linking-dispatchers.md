Please note that this feature is unnamed, and all undecided naming conventions will be surrounded by `@` symbols for ease of replacement.

# Background
Complications occur when using a job manager dispatcher such as `slurm_dispatcher` with a straight-ahead dispatcher such as `serial_geoips` or `serial_bash`, and it becomes clear that perhaps what it means to be a dispatcher has been lost. Thus the question arises: is a dispatcher a job manager or a job executor? 

The argument for a dispatcher being a job manager stems from the fact dispatchers execute things, and that to manage multiple jobs, a job manager such as `slurm` needs to be executed. 

The argument for a dispatcher being a job executor stems from the same principle that dispatchers execute things.

Both of these are correct! But a dispatcher cannot exist strictly as both an executor and a manager or flexibility is lost. The solution: we need to define *when* (not if[^1]) a dispatcher is acting as a job manager.

# The @bus@
So, the simple solution is for dispatchers to have an optional `@bus@` field that links to other dispatchers. For instance:
```
  - identifier: dispatcher-slurm-dispatcher
    spec:
      kind: dispatcher
      name: slurm_dispatcher
      config:
        poll_interval_seconds: 30.0
        max_concurrent_jobs: 10
        wait_for_completion: true
        submission_timeout_seconds: 60.0
        polling_timeout_seconds: 86400.0
        sbatch_extra_args:
        - PydanticUndefined
      @bus@: [dispatcher-serial-geoips]
  - identifier: dispatcher-serial-geoips
    spec:
      kind: dispatcher
      name: serial_geoips
      config:
        workflow_name: abi_airmass
        timeout_seconds: 3600.0
        log_to_file: false
        only_log_stderr: false
        scan_stderr: false
  allow_implicit_target: true
```
With the `@bus@` field, `slurm_dispatcher` knows what it's managing, and will (with added implementation) run each linked dispatcher accordingly.

# Problems with @bus@

## Arbitrary Python Execution
A glaring problem with @bus@ is the fact that some job managers such as `slurm` only accept files as inputs, and that Courier executes its dispatchers through their embedded functions. The current implementation for `slurm_dispatcher` accepts a string that acts as an in-place shell file. 

A solution to this comes from [this stackoverflow thread]("https://stackoverflow.com/questions/6036082/call-a-python-function-from-jinja2"), where inputted code is executed as a Jinja2 template.

## Circular Linking
Exception handling needs to be implemented for perhaps large service configs where two dispatchers with @bus@ functionality can link infinitely. Ignoring this could cause high CPU usage with no work being done, so it should be caught at runtime.

[^1]: Not if, because previously-implemented dispatchers that act as job managers should still keep their original implementation of both management and execution. The added functionality is the choice between the two.
