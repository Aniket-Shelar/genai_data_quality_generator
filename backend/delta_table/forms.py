from django import forms

class RulesApprovalForm(forms.Form):
    TABLE_NAMES = [
        ('', 'Select table'),
        ('insurance', 'Insurance'),
        ('property', 'Property'),
        ('person', 'Person'),
    ]
    
    table_names = forms.ChoiceField(choices=TABLE_NAMES)
