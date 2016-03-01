from api.exceptions import ParamException
from api.resources.base import BaseResource


class CreateResource(BaseResource):
    """
    Base class for resources responsible for creation of new objects
    """
    def __init__(self, objects_class, expected_params=[]):
        """
        :param objects_class: class represent single element of element that is planned to be created
        :param expected_params: list of params expected on obj creation
        """
        self.objects_class = objects_class
        self.expected_params = expected_params

    def clean(self, data, errors):
        """
        Might be useful to check 2 params at once or more complicated validations

        :param data: dict with cleaned values for each of `expected_params`
        :param errors: dict with errors
        :return: data and errors after additional cleanup if required
        """
        return data, errors

    def get_data(self, req, resp):
        """
        Get data defined in expected params, validate retrieved data
        and if something is not ok add errors to errors dict.

        You can write validation functions for your params by creating a :func:`clean_param_name`
        in your extension of :class:`CreateResource`. We will put value of the param to
        :func:`clean_param_name`. If :func:`clean_param_name` will raise `ParamException` raised error message
        will be added to errors of this params, otherwise :func:`clean_param_name` should return cleaned value
        from this param.

        :param req: falcon request object
        :param resp: falcon response object
        :return: dict with data to save / update and errors
        """
        d = {}
        errors = {}
        for param_name in self.get_expected_params(req=req, resp=resp):
            valid_func = getattr(self, 'clean_%s' % param_name, None)
            value = req.context['doc'].get(param_name)
            if valid_func:
                try:
                    value = valid_func(value)
                except ParamException as e:
                    errors[param_name] = str(e)
                else:
                    d[param_name] = value
            else:
                d[param_name] = value
        return self.clean(d, errors)

    def get_expected_params(self, req, resp):
        """Get list of expected params. By default it's just return declared params in `excpected_params`
        but you might want to make it dynamic base on `req` and `resp` objects

        :param req: falcon request object
        :param resp: falcon response object
        :return: list of expected params during save
        """
        return self.expected_params

    def on_put(self, req, resp):
        """
        Method run when put is send to api endpoint.
        Validate data and depends on validation send information
        about created object or errors list

        :param req: falcon request object
        :param resp: falcon response object
        """
        data, errors = self.get_data(req, resp)
        if errors:
            result = {'errors': errors}
        else:
            obj = self.save(**data)
            result = self.to_json(obj)
        self.render_response(
            result=result,
            req=req,
            resp=resp
        )

    def save(self, **kwargs):
        """
        :param kwargs: data of :class:`objects_class` to save
        :return: created instance of :class:`object_class`
        """
        obj = self.objects_class(**kwargs)
        return obj.save()
