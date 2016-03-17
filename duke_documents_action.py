# -*- coding: utf-8 -*-
from pskov.contrib.action_base import ActionBase
from tornado.gen import coroutine
from tornado.options import options
from pskov.rest.handlers.query_handler import QueryHandler
from pskov.utils.connection_manager import ConnectionManager
from pskov.utils.term_manager_client import TermManagerClient
import string
import json

class DukeDocumentAction(ActionBase):
    title = "Поиск дубликатов"
    subject = "state"
    group = "Документы"
    result_type = "job"

    @coroutine
    def get_args(self, props, opts, docid=None, tag=None):

        props["threshold"] = {
        "title": "Верхний порог",
        "type": "string"
        }

        opts["threshold"] = {
        "helper": "Верхний порог не должен быть выше 1 и ниже 0 (рекомендуемое значение ~0.7)"
        }

        props["maybethreshold"] = {
        "title": "Нижний порог порог",
        "type": "string"
        }

        opts["maybethreshold"] = {
        "helper": "Нижний порог не должен быть ниже 0 и выже верхнего порога (рекомендуемое значение ~0.5)"
        }

        """
        final_fields = {}
        qh = QueryHandler(self.handler.application, self.handler.request)
        query = qh.get_state_query()
        fields = yield qh._get_fields()
        for field in fields:
            if string.find(field.term, "facets") != -1:
                final_fields[field.term] = field.title

        for key in final_fields.keys():
            props[key] = {
            "title": final_fields[key],
            "type": "string"
            }
        """

    @coroutine
    def execute(self, docid=None, tag=None, **kwargs):

        final_fields = {}
        qh = QueryHandler(self.handler.application, self.handler.request)
        query = qh.get_state_query()
        fields = yield qh._get_fields()
        for field in fields:
            if string.find(field.term, "facets") != -1:
                final_fields[field.term] = field.title

        job = {"action": "find_duplicates_duke", "query": query[0].serialize(), "fields": json.dumps(final_fields)}
        self.handler.queue.put(job, tube=options.bs_bgworker_tube, priority=50, use_storage=True)


