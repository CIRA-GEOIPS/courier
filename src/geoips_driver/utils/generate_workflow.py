"""Utility functions used to dynamically generate a workflow plugin."""

from copy import deepcopy

from geoips.interfaces import workflows
from geoips.pydantic_models.v1.workflows import WorkflowPluginModel


def build_workflow(name):
    """Retrieve a workflow plugin from the given name.

    Parameters
    ----------
    name: str
        - The name of the workflow plugin we want to retrieve.

    Returns
    -------
    workflow: WorkflowsPlugin
        - A GeoIPS WorkflowsPlugin python object which lists plugins as steps in the
          order in which they should be executed.

    """
    workflow = workflows.get_plugin(name)
    steps = workflow.get("spec", {}).get("steps")
    final_steps = {}
    for key, step in steps.items():
        if step.get("kind") == "workflow":
            # Found a workflow step. Replace this with the contents of the workflow
            # requested
            workflow_name = step.get("name")
            workflow_spec = step.get("spec")
            # Using self here as embedded workflows could theoretically happen
            # infinitely. Recursion allows for this
            dsteps = workflows.get_plugin(workflow_name).get("spec", {}).get("steps")
            for dkey, dstep in dsteps.items():
                dstep_name = dstep.get("name")
                if "steps" in workflow_spec:
                    # Steps aren't required. For example, if you want all of the
                    # steps from a default workflow, don't bother specifying 'steps'
                    for okey, ostep in workflow_spec["steps"]:
                        ostep_name = ostep.get("name")
                        if ostep_name == dstep_name:
                            # If a key was found in both the default workflow and
                            # the workflow we're retrieving, then override the
                            # default recursively where conflicts occur. However,
                            # if a conflict is found, let's say for 'colormapper',
                            # don't just replace the default colormapper with the
                            # override colormapper. Only override where keys
                            # conflict, and add any default key / values that aren't
                            # present in the override dictionary
                            dsteps[dkey] = _deep_merge(deepcopy(dstep), ostep)
                else:
                    final_steps[dkey] = dstep
        else:
            final_steps[key] = step

    workflow["spec"]["steps"] = final_steps

    return workflow


def _deep_merge(default, override):
    """Recursively merges 'override' into 'default'.

    Keys in 'override' will override those in 'default', while preserving any keys
    in 'default' not present in 'override'.

    Parameters
    ----------
    default: dict
        - A dictionary whose values we will use as default
    override: dict
        - A dictionary whose values will override default if the same keys are found
    """
    for key, value in override.items():
        if (
            key in default
            and isinstance(default[key], dict)
            and isinstance(value, dict)
        ):
            # If both values are dictionaries, recursively merge
            _deep_merge(default[key], value)
        else:
            # Otherwise, override or add the key-value pair from override
            default[key] = value
    return default


def generate_workflow_from_steps(steps: dict) -> WorkflowPluginModel:
    """Generate a workflow plugin model from an arbitrary list of steps.

    Parameters
    ----------
    steps: dict
        - A dictionary of ordered steps used to dynamically generate a workflow plugin.

    Returns
    -------
    workflow: WorkflowPluginModel
        - The generated workflow plugin model.
    """
    workflow = {
        "interface": "workflows",
        "family": "order_based",
        "name": "generated",
        "docstring": "Dynamically generated workflow plugin.",
        "spec": {},
    }
    # The full and final ordered dictionary of steps to process
    final_steps = {}

    for key, step in steps.items():
        # For each step, check if it's a workflow plugin.

        # If it is, build that workflow plugin (embedded workflows will be unpacked
        # recursively). Add each step from that workflow in the exact order specified to
        # final_steps

        # Otherwise, just add the step as specified to final_steps
        if step.get("kind") == "workflow":
            unpacked_workflow = build_workflow(step.get("name"))
            for ukey, ustep in (
                unpacked_workflow.get("spec", {}).get("steps", {}).items()
            ):
                final_steps[ukey] = ustep

        else:
            final_steps[key] = step

    workflow["spec"]["steps"] = final_steps

    # NOTE: Need to either just send an unvalidated workflow dictionary or update logic
    # within bases.py to handle unregistered plugins.

    # workflow = WorkflowPluginModel(**workflow).model_dump()

    return workflow
