package main

// wayline — command-line interface for the Wayline DAG scheduling framework.
//
// Primary (kubectl-style) commands:
//
//	wayline apply  -f <file>                 Create/update an ODAG or ODAGTemplate
//	wayline get    [odags|templates] [-n]    List resources
//	wayline status <name> [-n]               Show detailed status of an ODAG
//	wayline logs   <odag> <task> [-n]        Stream logs from a task pod
//	wayline delete <name> | template <name>  Delete an ODAG (or template)
//	wayline run    <template> [-n]           Create a new run from an ODAGTemplate
//	wayline runs   <template> [-n]           List all runs of an ODAGTemplate
//	wayline show   <template> [-n]           Show ODAGTemplate detail + profile
//
// The legacy verb groups "wayline odag ..." and "wayline template ..." remain
// available as hidden aliases for backward compatibility.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	odagGVR         = schema.GroupVersionResource{Group: "wl.io", Version: "v1", Resource: "odags"}
	odagTemplateGVR = schema.GroupVersionResource{Group: "wl.io", Version: "v1", Resource: "odagtemplates"}
)

// globals set by persistent / per-command flags
var (
	kubeconfig string
	filename   string
)

func main() {
	root := &cobra.Command{
		Use:   "wayline",
		Short: "Wayline — a data-aware DAG scheduling framework for Kubernetes",
		Long: `Wayline schedules and runs one-shot task graphs (ODAGs) on Kubernetes.

Unlike artifact-store workflow engines, Wayline decouples task completion from
data availability: a per-node data-agent moves intermediate outputs directly
between nodes and exposes their readiness as scheduler-visible runtime state.
Specs are applied as Kubernetes custom resources (wl.io/v1) and managed by the
odag-controller.

Common usage:

  wayline apply  -f examples/dag-pipeline/odag.yml
  wayline get    odags
  wayline status dag-pipeline
  wayline logs   dag-pipeline generate
  wayline delete dag-pipeline`,
	}
	root.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", defaultKubeconfig(), "path to kubeconfig")

	// Primary kubectl-style verbs.
	root.AddCommand(
		applyCmd(),
		getCmd(),
		statusCmd(),
		logsCmd(),
		deleteCmd(),
		runCmd(),
		runsCmd(),
		showCmd(),
	)
	// Hidden legacy alias groups (back-compat).
	root.AddCommand(odagCmd(), templateCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// ─── Primary kubectl-style commands ─────────────────────────────────────────

func applyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "apply -f <file>",
		Short: "Create or update an ODAG or ODAGTemplate from a YAML file",
		Long:  "Apply a Wayline resource (ODAG or ODAGTemplate) from a YAML spec file. The kind is detected automatically; an existing resource of the same name is updated.",
		Example: `  wayline apply -f examples/dag-pipeline/odag.yml
  wayline apply -f examples/dag-pipeline/template.yml`,
		RunE: applyFromFile,
	}
	cmd.Flags().StringVarP(&filename, "file", "f", "", "path to ODAG/ODAGTemplate YAML (required)")
	cmd.Flags().StringP("namespace", "n", "", "namespace (default: from manifest, else 'default')")
	cmd.MarkFlagRequired("file")
	return cmd
}

func getCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get [odags|templates]",
		Short: "List ODAGs or ODAGTemplates",
		Long:  "List Wayline resources in a namespace. With no argument, lists ODAGs.",
		Example: `  wayline get odags
  wayline get templates -n wl-system`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res := "odags"
			if len(args) > 0 {
				res = args[0]
			}
			switch res {
			case "odag", "odags":
				return listResources(odagGVR)(cmd, args)
			case "template", "templates", "odagtemplate", "odagtemplates":
				return templateList(cmd, args)
			default:
				return fmt.Errorf("unknown resource %q (expected 'odags' or 'templates')", res)
			}
		},
	}
	cmd.Flags().StringP("namespace", "n", "default", "namespace")
	return cmd
}

func statusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "status <name>",
		Short:   "Show detailed status of an ODAG",
		Long:    "Show detailed status of an ODAG including per-task phase, assigned node, and timing.",
		Example: "  wayline status dag-pipeline",
		Args:    cobra.ExactArgs(1),
		RunE:    odagStatus,
	}
	cmd.Flags().StringP("namespace", "n", "default", "namespace")
	return cmd
}

func logsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "logs <odag-name> <task-name>",
		Short:   "Stream logs from an ODAG task pod",
		Long:    "Stream stdout/stderr from the pod running the specified task.",
		Example: "  wayline logs dag-pipeline generate",
		Args:    cobra.ExactArgs(2),
		RunE:    streamLogs("wl-odag"),
	}
	cmd.Flags().StringP("namespace", "n", "default", "namespace")
	return cmd
}

func deleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name> | delete template <name>",
		Short: "Delete an ODAG (and its pods/services) or an ODAGTemplate",
		Long:  "Delete an ODAG along with its task pods and services. To delete a template, use 'wayline delete template <name>'.",
		Example: `  wayline delete dag-pipeline
  wayline delete template dag-pipeline -n wl-system`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 2 {
				switch args[0] {
				case "template", "templates", "odagtemplate", "odagtemplates":
					return deleteTemplate(cmd, args[1])
				case "odag", "odags":
					return deleteResource(odagGVR)(cmd, args[1:])
				default:
					return fmt.Errorf("unknown resource %q (expected 'odag' or 'template')", args[0])
				}
			}
			return deleteResource(odagGVR)(cmd, args)
		},
	}
	cmd.Flags().StringP("namespace", "n", "default", "namespace")
	return cmd
}

func runCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "run <template-name>",
		Short:   "Create a new ODAG run from an ODAGTemplate",
		Long:    "Fetch the named ODAGTemplate and create a new ODAG run with an auto-assigned run number.",
		Example: "  wayline run dag-pipeline -n wl-system",
		Args:    cobra.ExactArgs(1),
		RunE:    odagRunFromTemplate,
	}
	cmd.Flags().StringP("namespace", "n", "wl-system", "namespace")
	return cmd
}

func runsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "runs <template-name>",
		Short:   "List all runs of an ODAGTemplate",
		Long:    "List all ODAG runs created from a template, showing run number, phase, makespan, and age.",
		Example: "  wayline runs dag-pipeline -n wl-system",
		Args:    cobra.ExactArgs(1),
		RunE:    odagListRuns,
	}
	cmd.Flags().StringP("namespace", "n", "wl-system", "namespace")
	return cmd
}

func showCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "show <template-name>",
		Short:   "Show ODAGTemplate detail + profile summary",
		Long:    "Show detailed information about an ODAGTemplate including profiling config, tasks, and the per-(task,node) profile summary.",
		Example: "  wayline show dag-pipeline -n wl-system",
		Args:    cobra.ExactArgs(1),
		RunE:    templateShow,
	}
	cmd.Flags().StringP("namespace", "n", "wl-system", "namespace")
	return cmd
}

// ─── Hidden legacy alias: wayline odag ... ──────────────────────────────────

func odagCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "odag",
		Short:  "Manage one-shot DAGs (legacy alias)",
		Hidden: true,
	}

	submit := &cobra.Command{
		Use:   "submit -f <file>",
		Short: "Submit an ODAG from a YAML file",
		RunE:  submitResource(odagGVR),
	}
	submit.Flags().StringVarP(&filename, "file", "f", "", "path to ODAG YAML (required)")
	submit.MarkFlagRequired("file")

	list := &cobra.Command{Use: "list", Short: "List ODAGs", RunE: listResources(odagGVR)}
	list.Flags().StringP("namespace", "n", "default", "namespace")

	status := &cobra.Command{Use: "status <name>", Short: "Show ODAG status", Args: cobra.ExactArgs(1), RunE: odagStatus}
	status.Flags().StringP("namespace", "n", "default", "namespace")

	del := &cobra.Command{Use: "delete <name>", Short: "Delete an ODAG and its task resources", Args: cobra.ExactArgs(1), RunE: deleteResource(odagGVR)}
	del.Flags().StringP("namespace", "n", "default", "namespace")

	logs := &cobra.Command{Use: "logs <odag-name> <task-name>", Short: "Stream logs from an ODAG task pod", Args: cobra.ExactArgs(2), RunE: streamLogs("wl-odag")}
	logs.Flags().StringP("namespace", "n", "default", "namespace")

	run := &cobra.Command{Use: "run <template-name>", Short: "Create a new run from an ODAGTemplate", Args: cobra.ExactArgs(1), RunE: odagRunFromTemplate}
	run.Flags().StringP("namespace", "n", "wl-system", "namespace")

	runs := &cobra.Command{Use: "runs <template-name>", Short: "List all runs of an ODAGTemplate", Args: cobra.ExactArgs(1), RunE: odagListRuns}
	runs.Flags().StringP("namespace", "n", "wl-system", "namespace")

	cmd.AddCommand(submit, list, status, del, logs, run, runs)
	return cmd
}

// ─── Implementations ─────────────────────────────────────────────────────────

// applyFromFile decodes a YAML manifest, routes by kind to the right resource,
// and creates it (or updates an existing resource of the same name).
func applyFromFile(cmd *cobra.Command, args []string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("reading %s: %w", filename, err)
	}
	dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	obj := &unstructured.Unstructured{}
	if _, _, err = dec.Decode(data, nil, obj); err != nil {
		return fmt.Errorf("parsing YAML: %w", err)
	}

	var gvr schema.GroupVersionResource
	switch obj.GetKind() {
	case "ODAG":
		gvr = odagGVR
	case "ODAGTemplate":
		gvr = odagTemplateGVR
	default:
		return fmt.Errorf("unsupported kind %q (expected ODAG or ODAGTemplate)", obj.GetKind())
	}

	ns := nsOf(cmd)
	if ns == "" {
		ns = obj.GetNamespace()
	}
	if ns == "" {
		ns = "default"
	}
	obj.SetNamespace(ns)

	dc, err := dynClient()
	if err != nil {
		return err
	}
	ri := dc.Resource(gvr).Namespace(ns)

	// Create-or-update: if a resource of this name exists, carry its
	// resourceVersion and update; otherwise create.
	if name := obj.GetName(); name != "" {
		if existing, getErr := ri.Get(context.Background(), name, metav1.GetOptions{}); getErr == nil {
			obj.SetResourceVersion(existing.GetResourceVersion())
			if _, err = ri.Update(context.Background(), obj, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("update: %w", err)
			}
			fmt.Printf("%s/%s configured\n", gvr.Resource, name)
			return nil
		}
	}
	result, err := ri.Create(context.Background(), obj, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	fmt.Printf("%s/%s created\n", gvr.Resource, result.GetName())
	return nil
}

func submitResource(gvr schema.GroupVersionResource) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("reading %s: %w", filename, err)
		}
		dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
		obj := &unstructured.Unstructured{}
		if _, _, err = dec.Decode(data, nil, obj); err != nil {
			return fmt.Errorf("parsing YAML: %w", err)
		}
		ns := obj.GetNamespace()
		if ns == "" {
			ns = "default"
		}
		dc, err := dynClient()
		if err != nil {
			return err
		}
		result, err := dc.Resource(gvr).Namespace(ns).Create(context.Background(), obj, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create: %w", err)
		}
		fmt.Printf("%s/%s submitted\n", gvr.Resource, result.GetName())
		return nil
	}
}

func listResources(gvr schema.GroupVersionResource) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		dc, err := dynClient()
		if err != nil {
			return err
		}
		list, err := dc.Resource(gvr).Namespace(nsOf(cmd)).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "NAME\tNAMESPACE\tPHASE\tAGE")
		for _, item := range list.Items {
			phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
			if phase == "" {
				phase = "Pending"
			}
			age := fmtAge(item.GetCreationTimestamp().Time)
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", item.GetName(), item.GetNamespace(), phase, age)
		}
		return w.Flush()
	}
}

func odagStatus(cmd *cobra.Command, args []string) error {
	name := args[0]
	dc, err := dynClient()
	if err != nil {
		return err
	}
	obj, err := dc.Resource(odagGVR).Namespace(nsOf(cmd)).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	makespan, _, _ := unstructured.NestedFloat64(obj.Object, "status", "makespan")
	startTime, _, _ := unstructured.NestedString(obj.Object, "status", "startTime")
	completionTime, _, _ := unstructured.NestedString(obj.Object, "status", "completionTime")
	message, _, _ := unstructured.NestedString(obj.Object, "status", "message")

	fmt.Printf("Name:        %s\n", name)
	fmt.Printf("Namespace:   %s\n", nsOf(cmd))
	fmt.Printf("Phase:       %s\n", phase)
	if makespan > 0 {
		fmt.Printf("Makespan:    %.1fs\n", makespan)
	}
	if startTime != "" {
		fmt.Printf("Start:       %s\n", startTime)
	}
	if completionTime != "" {
		fmt.Printf("Completion:  %s\n", completionTime)
	}
	if message != "" {
		fmt.Printf("Message:     %s\n", message)
	}

	// Print per-task status if available
	tasks, found, _ := unstructured.NestedSlice(obj.Object, "status", "tasks")
	if found && len(tasks) > 0 {
		fmt.Println("\nTasks:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "  NAME\tPHASE\tNODE")
		for _, t := range tasks {
			tm, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			tname, _ := tm["name"].(string)
			tphase, _ := tm["phase"].(string)
			tnode, _ := tm["node"].(string)
			fmt.Fprintf(w, "  %s\t%s\t%s\n", tname, tphase, tnode)
		}
		w.Flush()
	}

	// Show spec tasks from spec
	specTasks, found, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	if found {
		fmt.Printf("\nSpec tasks (%d):\n", len(specTasks))
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "  NAME\tIMAGE\tDEPS")
		for _, t := range specTasks {
			tm, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			tname, _ := tm["name"].(string)
			timage, _ := tm["image"].(string)
			deps, _ := tm["dependencies"].([]interface{})
			depNames := ""
			for _, d := range deps {
				if depNames != "" {
					depNames += ","
				}
				depNames += fmt.Sprint(d)
			}
			if depNames == "" {
				depNames = "-"
			}
			fmt.Fprintf(w, "  %s\t%s\t%s\n", tname, timage, depNames)
		}
		w.Flush()
	}
	return nil
}

func deleteResource(gvr schema.GroupVersionResource) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		name := args[0]
		dc, err := dynClient()
		if err != nil {
			return err
		}
		if err = dc.Resource(gvr).Namespace(nsOf(cmd)).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("delete %s %s: %w", gvr.Resource, name, err)
		}

		// Also delete associated pods and services
		kc, err := k8sClient()
		if err != nil {
			return err
		}
		sel := fmt.Sprintf("wl-odag=%s", name)
		_ = kc.CoreV1().Pods(nsOf(cmd)).DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: sel})
		svcs, _ := kc.CoreV1().Services(nsOf(cmd)).List(context.Background(), metav1.ListOptions{LabelSelector: sel})
		for _, svc := range svcs.Items {
			_ = kc.CoreV1().Services(nsOf(cmd)).Delete(context.Background(), svc.Name, metav1.DeleteOptions{})
		}

		fmt.Printf("%s/%s deleted\n", gvr.Resource, name)
		return nil
	}
}

func deleteTemplate(cmd *cobra.Command, name string) error {
	dc, err := dynClient()
	if err != nil {
		return err
	}
	if err = dc.Resource(odagTemplateGVR).Namespace(nsOf(cmd)).Delete(
		context.Background(), name, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("delete template %s: %w", name, err)
	}
	fmt.Printf("odagtemplate/%s deleted\n", name)
	return nil
}

func streamLogs(labelKey string) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		dagName, taskName := args[0], args[1]
		kc, err := k8sClient()
		if err != nil {
			return err
		}

		// Find pod: name is {dag-name}-{task-name} or use label selector
		podName := fmt.Sprintf("%s-%s", dagName, taskName)
		pods, err := kc.CoreV1().Pods(nsOf(cmd)).List(context.Background(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("%s=%s,wl-task=%s", labelKey, dagName, taskName),
		})
		if err == nil && len(pods.Items) > 0 {
			podName = pods.Items[0].Name
		}

		req := kc.CoreV1().Pods(nsOf(cmd)).GetLogs(podName, &corev1.PodLogOptions{Follow: true})
		stream, err := req.Stream(context.Background())
		if err != nil {
			return fmt.Errorf("logs for %s: %w", podName, err)
		}
		defer stream.Close()

		scanner := bufio.NewScanner(stream)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
		return scanner.Err()
	}
}

// ─── Hidden legacy alias: wayline template ... ──────────────────────────────

func templateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "template",
		Short:  "Manage ODAG templates (legacy alias)",
		Hidden: true,
	}

	apply := &cobra.Command{
		Use:   "apply -f <file>",
		Short: "Register an ODAGTemplate from a YAML file",
		RunE:  submitResource(odagTemplateGVR),
	}
	apply.Flags().StringVarP(&filename, "file", "f", "", "path to ODAGTemplate YAML (required)")
	apply.MarkFlagRequired("file")

	list := &cobra.Command{Use: "list", Short: "List ODAGTemplates", RunE: templateList}
	list.Flags().StringP("namespace", "n", "wl-system", "namespace")

	show := &cobra.Command{Use: "show <name>", Short: "Show ODAGTemplate detail", Args: cobra.ExactArgs(1), RunE: templateShow}
	show.Flags().StringP("namespace", "n", "wl-system", "namespace")

	del := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete an ODAGTemplate",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return deleteTemplate(cmd, args[0])
		},
	}
	del.Flags().StringP("namespace", "n", "wl-system", "namespace")

	cmd.AddCommand(apply, list, show, del)
	return cmd
}

func templateList(cmd *cobra.Command, args []string) error {
	dc, err := dynClient()
	if err != nil {
		return err
	}
	list, err := dc.Resource(odagTemplateGVR).Namespace(nsOf(cmd)).List(
		context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NAME\tSCHEDULER\tTASKS\tRUNS\tLAST MAKESPAN\tPROFILING\tAGE")
	for _, item := range list.Items {
		scheduler, _, _ := unstructured.NestedString(item.Object, "spec", "scheduler")
		tasks, _, _ := unstructured.NestedSlice(item.Object, "spec", "tasks")
		runCount, _, _ := unstructured.NestedInt64(item.Object, "status", "runCount")
		makespan, _, _ := unstructured.NestedFloat64(item.Object, "status", "lastRunMakespan")
		age := fmtAge(item.GetCreationTimestamp().Time)

		makespanStr := "-"
		if makespan > 0 {
			makespanStr = fmt.Sprintf("%.1fs", makespan)
		}
		profilingStr := "yes"
		if p, ok, _ := unstructured.NestedBool(item.Object, "spec", "profiling", "enabled"); ok && !p {
			profilingStr = "no"
		}

		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\t%s\n",
			item.GetName(), scheduler, len(tasks), runCount, makespanStr, profilingStr, age)
	}
	return w.Flush()
}

func templateShow(cmd *cobra.Command, args []string) error {
	name := args[0]
	dc, err := dynClient()
	if err != nil {
		return err
	}
	obj, err := dc.Resource(odagTemplateGVR).Namespace(nsOf(cmd)).Get(
		context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	scheduler, _, _ := unstructured.NestedString(obj.Object, "spec", "scheduler")
	desc, _, _ := unstructured.NestedString(obj.Object, "spec", "description")
	runCount, _, _ := unstructured.NestedInt64(obj.Object, "status", "runCount")
	lastRunName, _, _ := unstructured.NestedString(obj.Object, "status", "lastRunName")
	lastRunPhase, _, _ := unstructured.NestedString(obj.Object, "status", "lastRunPhase")
	lastMakespan, _, _ := unstructured.NestedFloat64(obj.Object, "status", "lastRunMakespan")

	fmt.Printf("Name:         %s\n", name)
	fmt.Printf("Namespace:    %s\n", nsOf(cmd))
	if desc != "" {
		fmt.Printf("Description:  %s\n", desc)
	}
	fmt.Printf("Scheduler:    %s\n", scheduler)
	fmt.Printf("Runs:         %d\n", runCount)
	if lastRunName != "" {
		fmt.Printf("Last Run:     %s (%s, %.1fs)\n", lastRunName, lastRunPhase, lastMakespan)
	}

	// Profiling config
	fmt.Println("\nProfiling:")
	enabled := true
	if v, ok, _ := unstructured.NestedBool(obj.Object, "spec", "profiling", "enabled"); ok {
		enabled = v
	}
	fmt.Printf("  Enabled:      %v\n", enabled)
	if warmup, ok, _ := unstructured.NestedInt64(obj.Object, "spec", "profiling", "warmupRuns"); ok {
		fmt.Printf("  Warmup Runs:  %d\n", warmup)
	}
	if minS, ok, _ := unstructured.NestedInt64(obj.Object, "spec", "profiling", "minSamples"); ok {
		fmt.Printf("  Min Samples:  %d\n", minS)
	}
	if alpha, ok, _ := unstructured.NestedFloat64(obj.Object, "spec", "profiling", "emaAlpha"); ok {
		fmt.Printf("  EMA Alpha:    %.2f\n", alpha)
	}
	if maxS, ok, _ := unstructured.NestedInt64(obj.Object, "spec", "profiling", "maxSamples"); ok {
		fmt.Printf("  Max Samples:  %d\n", maxS)
	}

	// Tasks
	specTasks, _, _ := unstructured.NestedSlice(obj.Object, "spec", "tasks")
	fmt.Printf("\nTasks (%d):\n", len(specTasks))
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "  NAME\tIMAGE\tRUNTIME\tDATA SIZE\tDEPS\tCONSTRAINTS")
	for _, t := range specTasks {
		tm, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		tname, _ := tm["name"].(string)
		timage, _ := tm["image"].(string)

		runtimeStr := "-"
		if rt, ok := tm["runtime"].(int64); ok {
			runtimeStr = fmt.Sprintf("%ds", rt)
		} else if rt, ok := tm["runtime"].(float64); ok {
			runtimeStr = fmt.Sprintf("%.0fs", rt)
		}

		dataSize, _ := tm["dataSize"].(string)
		if dataSize == "" {
			dataSize = "-"
		}

		deps, _ := tm["dependencies"].([]interface{})
		depStr := "-"
		if len(deps) > 0 {
			names := make([]string, len(deps))
			for i, d := range deps {
				names[i] = fmt.Sprint(d)
			}
			depStr = fmt.Sprintf("%v", names)
		}

		constraints, _, _ := unstructured.NestedStringSlice(tm, "constraints", "nodeNames")
		constraintStr := "-"
		if len(constraints) > 0 {
			constraintStr = fmt.Sprintf("%v", constraints)
		}

		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\t%s\n",
			tname, timage, runtimeStr, dataSize, depStr, constraintStr)
	}
	w.Flush()

	// Profile summary
	profileSummary, ok, _ := unstructured.NestedMap(obj.Object, "status", "profileSummary")
	if ok && len(profileSummary) > 0 {
		fmt.Println("\nProfile Summary (task → node → runtime):")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "  TASK\tNODE\tRUNTIME (EMA)")
		for task, nodeMap := range profileSummary {
			nm, ok := nodeMap.(map[string]interface{})
			if !ok {
				continue
			}
			for node, runtime := range nm {
				fmt.Fprintf(w, "  %s\t%s\t%.2fs\n", task, node, toFloat64(runtime))
			}
		}
		w.Flush()
	}

	return nil
}

// ─── ODAG run from template ──────────────────────────────────────────────────

func odagRunFromTemplate(cmd *cobra.Command, args []string) error {
	templateName := args[0]
	dc, err := dynClient()
	if err != nil {
		return err
	}

	// Fetch the template.
	tmpl, err := dc.Resource(odagTemplateGVR).Namespace(nsOf(cmd)).Get(
		context.Background(), templateName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get template %s: %w", templateName, err)
	}

	// Extract spec from template, stripping template-only fields.
	spec, _, err := unstructured.NestedMap(tmpl.Object, "spec")
	if err != nil {
		return fmt.Errorf("extract spec: %w", err)
	}
	delete(spec, "profiling")
	delete(spec, "defaults")
	delete(spec, "retention")
	delete(spec, "description")

	// Use generateName so K8s assigns a unique suffix. The persistent run
	// number (wl.io/run) is stamped by the controller from its SQL counter
	// on first reconcile — see deployODAG in cmd/odag-controller/main.go.
	// Computing the number locally from a live-resource list is racy: any
	// prior run that's been deleted resets the count and produces duplicate
	// names that collide in wl-history.db.
	odag := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "wl.io/v1",
			"kind":       "ODAG",
			"metadata": map[string]interface{}{
				"generateName": fmt.Sprintf("%s-run-", templateName),
				"namespace":    nsOf(cmd),
				"labels": map[string]interface{}{
					"wl.io/template": templateName,
				},
			},
			"spec": spec,
		},
	}

	created, err := dc.Resource(odagGVR).Namespace(nsOf(cmd)).Create(
		context.Background(), odag, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create run: %w", err)
	}

	fmt.Printf("Created run %s (from template %s)\n", created.GetName(), templateName)
	return nil
}

func odagListRuns(cmd *cobra.Command, args []string) error {
	templateName := args[0]
	dc, err := dynClient()
	if err != nil {
		return err
	}

	list, err := dc.Resource(odagGVR).Namespace(nsOf(cmd)).List(
		context.Background(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("wl.io/template=%s", templateName),
		})
	if err != nil {
		return err
	}

	if len(list.Items) == 0 {
		fmt.Printf("No runs found for template %s in namespace %s\n", templateName, nsOf(cmd))
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NAME\tRUN\tPHASE\tMAKESPAN\tAGE")
	for _, item := range list.Items {
		labels := item.GetLabels()
		runNum := labels["wl.io/run"]
		phase, _, _ := unstructured.NestedString(item.Object, "status", "phase")
		if phase == "" {
			phase = "Pending"
		}
		makespan, _, _ := unstructured.NestedFloat64(item.Object, "status", "makespan")
		makespanStr := "-"
		if makespan > 0 {
			makespanStr = fmt.Sprintf("%.1fs", makespan)
		}
		age := fmtAge(item.GetCreationTimestamp().Time)
		fmt.Fprintf(w, "%s\t#%s\t%s\t%s\t%s\n", item.GetName(), runNum, phase, makespanStr, age)
	}
	return w.Flush()
}

// toFloat64 converts an interface{} (int64, float64, json.Number) to float64.
func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	default:
		return 0
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func dynClient() (dynamic.Interface, error) {
	cfg, err := buildConfig()
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(cfg)
}

func k8sClient() (*kubernetes.Clientset, error) {
	cfg, err := buildConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

func buildConfig() (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}
	return clientcmd.BuildConfigFromFlags("", defaultKubeconfig())
}

func defaultKubeconfig() string {
	if kc := os.Getenv("KUBECONFIG"); kc != "" {
		return kc
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".kube", "config")
}

// nsOf returns the namespace flag value for the invoked command. Each command
// registers its own "namespace" flag (so per-command defaults don't leak
// through a shared variable); this reads the one belonging to cmd.
func nsOf(cmd *cobra.Command) string {
	v, _ := cmd.Flags().GetString("namespace")
	return v
}

func fmtAge(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}
