/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Spin up a Redis cluster within the Kubernetes cluster using the bitami
// helm chart.
resource "helm_release" "redis" {
  wait       = false
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "redis"
  name       = "redis"
  namespace  = data.kubernetes_namespace.default.metadata[0].name
  set {
    name  = "auth.enabled"
    value = false
  }
  set {
    name  = "auth.sentinel"
    value = false
  }
}