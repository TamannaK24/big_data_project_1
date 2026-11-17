import argparse
import csv
from collections import Counter
from pathlib import Path

CLASSES = ('World', 'Sports', 'Business', 'Sci/Tech')
ALLOWED = set(CLASSES) | {'None of these'}


def main():
    parser = argparse.ArgumentParser(description='Evaluate 50 manually labeled Hacker News messages.')
    parser.add_argument('--input', default='outputs/manual_labels_50.csv')
    parser.add_argument('--output-dir', default='outputs')
    args = parser.parse_args()

    with open(args.input, newline='', encoding='utf-8-sig') as file:
        rows = list(csv.DictReader(file))

    if len(rows) != 50:
        raise ValueError(f'Expected 50 manually labeled messages; found {len(rows)}')
    invalid = [row['id'] for row in rows if row['manual_label'].strip() not in ALLOWED]
    if invalid:
        raise ValueError(f'Missing or invalid manual labels in IDs: {invalid}')
    invalid_predictions = [row['id'] for row in rows if row['predicted_class'].strip() not in CLASSES]
    if invalid_predictions:
        raise ValueError(f'Invalid predicted categories in IDs: {invalid_predictions}')

    for row in rows:
        row['manual_label'] = row['manual_label'].strip()
        row['predicted_class'] = row['predicted_class'].strip()
        row['evaluation'] = ('Excluded' if row['manual_label'] == 'None of these'
                             else 'Correct' if row['manual_label'] == row['predicted_class'] else 'Incorrect')

    counts = Counter(row['evaluation'] for row in rows)
    valid = counts['Correct'] + counts['Incorrect']
    accuracy = counts['Correct'] / valid if valid else 0
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    with (out / 'manual_evaluation.csv').open('w', newline='', encoding='utf-8-sig') as file:
        writer = csv.DictWriter(file, fieldnames=['id', 'text', 'predicted_class', 'probability', 'manual_label', 'evaluation'])
        writer.writeheader()
        writer.writerows(rows)

    with (out / 'confusion_matrix.csv').open('w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file)
        writer.writerow(['Manual label / Predicted label', *CLASSES])
        for manual in CLASSES:
            writer.writerow([manual, *[sum(row['manual_label'] == manual and row['predicted_class'] == predicted for row in rows) for predicted in CLASSES]])

    errors = [row for row in rows if row['evaluation'] == 'Incorrect']
    lines = [
        'HACKER NEWS MANUAL EVALUATION',
        f'Total manually labeled messages: {len(rows)}',
        f'None of these (excluded): {counts["Excluded"]}',
        f'Messages in four AG News categories: {valid}',
        f'Correct predictions: {counts["Correct"]}',
        f'Incorrect predictions: {counts["Incorrect"]}',
        f'Accuracy on applicable messages: {accuracy:.2%}',
        '',
        'MANUAL LABEL COUNTS',
    ]
    for label in [*CLASSES, 'None of these']:
        lines.append(f'{label}: {sum(row["manual_label"] == label for row in rows)}')
    lines += ['', 'MISCLASSIFIED EXAMPLES']
    for row in errors[:6]:
        lines.append(f'ID {row["id"]} | predicted={row["predicted_class"]} | actual={row["manual_label"]} | confidence={row["probability"]}')
        lines.append(f'  Text: {row["text"][:280]}')

    report = '\n'.join(lines) + '\n'
    (out / 'evaluation_summary.txt').write_text(report, encoding='utf-8')
    print(report)
    print('Saved:', out / 'manual_evaluation.csv')
    print('Saved:', out / 'confusion_matrix.csv')
    print('Saved:', out / 'evaluation_summary.txt')


if __name__ == '__main__':
    main()
