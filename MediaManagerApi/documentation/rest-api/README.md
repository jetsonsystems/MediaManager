# REST API

## Contents

  * [Overview](#overview)
  * [API Characteristics](./api-characteristics.md)
    * [RESTful Resource URLs](./api-characteristics.md#restful-resource-urls)
  * [API Resource Formats](./resource-formats.md)
  * [API Endpoints](./endpoints/README.md)

## Overview

The **PLM Media Manager - REST API** is a web based API which is RESTful in nature. The API provides access to elementary resources, such as images, document importers, storage synchronizers, etc.. In the current initial implementation all request and response formats are JSON. Note, it is anticipated that as the API evolves, not all features can be exposed in a RESTful manner, but where appropriate and possible, access to data is facilitated via endpoints which reference a RESTful resource. The REST API is complimented by the [PLM Media Manager - Notifications API](../notifications-api/README.md) in order to get feedback on operations which may have been triggered by the REST API, and may be completely asynchronously.
