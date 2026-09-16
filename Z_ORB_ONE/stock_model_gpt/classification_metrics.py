"""Count-based metrics shared by daily validation and pooled backtests."""
from .signals import CLASSES


def rates(tp, fp, fn):
    return {"tp": tp, "fp": fp, "fn": fn,
            "precision": tp / (tp + fp) if tp + fp else None,
            "recall": tp / (tp + fn) if tp + fn else None}


def matrix_metrics(matrix):
    total = sum(map(sum, matrix))
    per_class = {}
    for i, c in enumerate(CLASSES):
        support = sum(matrix[i])
        tp = matrix[i][i]
        per_class[str(c)] = {**rates(tp, sum(row[i] for row in matrix) - tp, support - tp),
                             "support": support}
    return {"classes": list(CLASSES), "confusion_matrix": matrix,
            "accuracy": sum(matrix[i][i] for i in range(5)) / total if total else None,
            "per_class": per_class}


def pool_matrices(matrices):
    return [[sum(matrix[i][j] for matrix in matrices) for j in range(5)] for i in range(5)]
