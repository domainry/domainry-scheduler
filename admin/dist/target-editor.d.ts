export interface SchedulerTargetEditorValue {
    targetType: string;
    targetKey: string;
    connectionKey: string;
    operation: string;
    dispatchMode: string;
}
export interface SchedulerTargetEditorLabels {
    targetType: string;
    targetKey: string;
    targetPlaceholder: string;
    targetLoading: string;
    connectionKey: string;
    operation: string;
    dispatchMode: string;
    runtimeCallback: string;
    direct: string;
}
export declare function SchedulerTargetEditor({ value, targetTypes, targetValues, targetLoading, errors, labels, onChange }: {
    value: SchedulerTargetEditorValue;
    targetTypes: string[];
    targetValues: string[];
    targetLoading?: boolean;
    errors?: Partial<Record<keyof SchedulerTargetEditorValue, string>>;
    labels: SchedulerTargetEditorLabels;
    onChange(next: SchedulerTargetEditorValue): void;
}): import("react").JSX.Element;
