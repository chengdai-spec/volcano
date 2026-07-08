/*
Copyright 2019 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/client/clientset/versioned"
	"volcano.sh/volcano/cmd/webhook-manager/app/options"
	"volcano.sh/volcano/pkg/webhooks/router"
)

const volcanoAdmissionPrefix = "volcano-admission-service"

func addCaCertForWebhook(kubeClient *kubernetes.Clientset, service *router.AdmissionService, caBundle []byte) error {
	if service.MutatingConfig != nil {
		// update MutatingWebhookConfigurations
		var mutatingWebhookName = volcanoAdmissionPrefix + strings.ReplaceAll(service.Path, "/", "-")
		var mutatingWebhook *v1.MutatingWebhookConfiguration
		webhookChanged := false
		if err := wait.PollUntilContextTimeout(context.Background(), time.Second, 5*time.Minute, true, func(_ context.Context) (done bool, err error) {
			mutatingWebhook, err = kubeClient.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(context.TODO(), mutatingWebhookName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					klog.Errorln(err)
					return false, nil
				}
				return false, fmt.Errorf("failed to get mutating webhook %v", err)
			}
			return true, nil
		}); err != nil {
			return fmt.Errorf("failed to get mutating webhook %v", err)
		}

		for index := 0; index < len(mutatingWebhook.Webhooks); index++ {
			if mutatingWebhook.Webhooks[index].ClientConfig.CABundle == nil ||
				!bytes.Equal(mutatingWebhook.Webhooks[index].ClientConfig.CABundle, caBundle) {
				mutatingWebhook.Webhooks[index].ClientConfig.CABundle = caBundle
				webhookChanged = true
			}
		}
		if webhookChanged {
			if _, err := kubeClient.AdmissionregistrationV1().MutatingWebhookConfigurations().Update(context.TODO(), mutatingWebhook, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("failed to update mutating admission webhooks %v %v", mutatingWebhookName, err)
			}
		}
	}

	if service.ValidatingConfig != nil {
		// update ValidatingWebhookConfigurations
		var validatingWebhookName = volcanoAdmissionPrefix + strings.ReplaceAll(service.Path, "/", "-")
		var validatingWebhook *v1.ValidatingWebhookConfiguration
		webhookChanged := false
		if err := wait.PollUntilContextTimeout(context.Background(), time.Second, 5*time.Minute, true, func(_ context.Context) (done bool, err error) {
			validatingWebhook, err = kubeClient.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.TODO(), validatingWebhookName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					klog.Errorln(err)
					return false, nil
				}
				return false, fmt.Errorf("failed to get validating webhook %v", err)
			}
			return true, nil
		}); err != nil {
			return fmt.Errorf("failed to get validating webhook %v", err)
		}

		for index := 0; index < len(validatingWebhook.Webhooks); index++ {
			if validatingWebhook.Webhooks[index].ClientConfig.CABundle == nil ||
				!bytes.Equal(validatingWebhook.Webhooks[index].ClientConfig.CABundle, caBundle) {
				validatingWebhook.Webhooks[index].ClientConfig.CABundle = caBundle
				webhookChanged = true
			}
		}
		if webhookChanged {
			if _, err := kubeClient.AdmissionregistrationV1().ValidatingWebhookConfigurations().Update(context.TODO(), validatingWebhook, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("failed to update validating admission webhooks %v %v", validatingWebhookName, err)
			}
		}
	}

	return nil
}

// getKubeClient Get a clientset with restConfig.
func getKubeClient(restConfig *rest.Config) *kubernetes.Clientset {
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		klog.Fatal(err)
	}
	return clientset
}

// getVolcanoClient get a clientset for volcano.
func getVolcanoClient(restConfig *rest.Config) *versioned.Clientset {
	clientset, err := versioned.NewForConfig(restConfig)
	if err != nil {
		klog.Fatal(err)
	}
	return clientset
}

// configTLS 是一个辅助函数，用于生成 TLS 配置。
// 它支持两种证书来源：
// 1. 直接通过 config 传入的证书数据（CertData/KeyData/CaCertData）
// 2. 如果 config 中没有，则尝试从 restConfig 中获取证书数据
//
// 该函数通常用于 Webhook 服务开启 HTTPS/TLS。
func configTLS(config *options.Config, restConfig *rest.Config) *tls.Config {
	// -------------------------------------------------------
	// 第一种情况：config 中显式提供了证书和私钥
	//
	// 例如：
	//   config.CertData = 服务端证书
	//   config.KeyData  = 服务端私钥
	//   config.CaCertData = CA 证书
	//
	// 这是最完整、最常见的一种 TLS 配置方式
	// -------------------------------------------------------
	if len(config.CertData) != 0 && len(config.KeyData) != 0 {
		// 创建一个证书池，用于保存 CA 证书
		// 这里的 CA 证书通常用于验证客户端证书，或者作为信任根
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(config.CaCertData)

		// 将 PEM 格式的证书和私钥解析成一个 tls.Certificate
		sCert, err := tls.X509KeyPair(config.CertData, config.KeyData)
		if err != nil {
			// 如果证书格式不合法、证书和私钥不匹配，直接退出
			klog.Fatal(err)
		}

		// 返回完整的 TLS 配置
		return &tls.Config{
			// 服务端证书链
			Certificates: []tls.Certificate{sCert},

			// 信任的根 CA 证书池
			RootCAs: certPool,

			// 最低 TLS 版本限制，避免使用过低版本
			MinVersion: tls.VersionTLS12,

			// 客户端证书认证策略：
			// 如果客户端提供证书，则验证；
			// 如果客户端不提供证书，也允许连接
			ClientAuth: tls.VerifyClientCertIfGiven,

			// 限定允许的加密套件
			CipherSuites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			},
		}
	}

	// -------------------------------------------------------
	// 第二种情况：config 中没有证书，则尝试从 restConfig 读取
	//
	// 这通常意味着证书是在 kubeconfig 或其他客户端配置里带进来的
	// -------------------------------------------------------
	if len(restConfig.CertData) != 0 && len(restConfig.KeyData) != 0 {
		// 将 restConfig 中的 PEM 证书和私钥解析为 tls.Certificate
		sCert, err := tls.X509KeyPair(restConfig.CertData, restConfig.KeyData)
		if err != nil {
			klog.Fatal(err)
		}

		// 返回一个最基本的 TLS 配置
		// 这里只设置了服务端证书，其他参数使用默认值
		return &tls.Config{
			Certificates: []tls.Certificate{sCert},
		}
	}

	// -------------------------------------------------------
	// 如果两种来源都没有证书数据，则无法启用 TLS
	// 因此直接退出
	// -------------------------------------------------------
	klog.Fatal("tls: failed to find any tls config data")
	return &tls.Config{}
}
