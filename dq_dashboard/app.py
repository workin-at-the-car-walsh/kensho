from dash import Dash, html, dcc, dash_table
from dash.dependencies import Input, Output
import dash_daq as daq
import pandas
import plotly.graph_objects as go
import awswrangler

wonderful_lime_green = '#77ca3b'
wonderful_bright_yellow = '#ffc840'
wonderful_orange = '#e34e32'
wonderful_silver = '#bdc4bb'
wonderful_dark_grey = '#595e5c'


def process_row(row, policy_and_roll_up_dict):
    
    rule_id = row['rule_id']
    constraint_status = row['constraint_status']
    if constraint_status == 'Success':
        return rule_id, [0, 0, 0, 0, 0, 0, 1, 0, 0]
    elif constraint_status != 'Failure':
        raise ValueError('{} is regarded as an invalid constraint status'.format(constraint_status))
    else:
        
        alert_level = policy_and_roll_up_dict[rule_id]['alert_level']
        
        if alert_level == 'error':
            error_tier = policy_and_roll_up_dict[rule_id]['error_tier']
            res = [0, 0, 0, 0, 0, 0, 0, 0, 0]
            res[int(error_tier) - 1] = 1
            return rule_id, res
        elif alert_level == 'warning':
            
            beta = policy_and_roll_up_dict[rule_id]['threshold']
            k = policy_and_roll_up_dict[rule_id]['k']
            n_epsilon = policy_and_roll_up_dict[rule_id]['n_epsilon']
            n_significant = policy_and_roll_up_dict[rule_id]['n_significant']
            
            v = row['validation_metric_value']
            n_warnings = 1. / k * abs(v - beta)
            
            res = [0, 0, 0, 0, 0, 0, 0, 0, 0]
            
            if n_warnings <= n_epsilon:
                res[5] = 1
            elif n_warnings < n_significant:
                res[4] = 1
            else:
                res[3] = 1
            
            return rule_id, res


def process_policy_and_roll_up(policy_and_roll_up_fp):

    policy_and_roll_up_df = pandas.read_csv(policy_and_roll_up_fp)

    policy_and_roll_up_dict = {
        v[0]: dict(zip(list(policy_and_roll_up_df.columns)[1:], v[1:]))
        for v in policy_and_roll_up_df.values
    }

    return policy_and_roll_up_dict


def roll_up_rows(df, policy_and_roll_up_dict):
    
    roll_up_items = [
        process_row(row, policy_and_roll_up_dict)
        for i, row in df.iterrows()
    ]

    traffic_light_column_names = ['red_1', 'red_2', 'red_3', 'amber_1', 'amber_2', 'amber_3', 'green_1', 'green_2', 'green_3'][::-1]
    traffic_light_df = pandas.DataFrame(
        [[k]+v[::-1] for k,v in roll_up_items],
        columns=['rule_id'] + traffic_light_column_names
    )

    #traffic_light_summary = traffic_light_df[traffic_light_column_names]
    traffic_light_df['extract_dt'] = df['extract_dt'].values[0]
    return traffic_light_df


RAG_COLOR_MAP = {
    'red': wonderful_orange,
    'amber': wonderful_bright_yellow,
    'green': wonderful_lime_green,
}


FAIL_SUCCESS_COLOR_MAP = {
    'Failure': wonderful_orange,
    'Success': wonderful_lime_green
}

RAG_LEVELS = ['no_data', 'red_1', 'red_2', 'red_3', 'amber_1', 'amber_2', 'amber_3', 'green_1', 'green_2', 'green_3']
RED_OR_AMBER = {'red_1', 'red_2', 'red_3', 'amber_1', 'amber_2', 'amber_3'}
RAG_LEVEL_NUMERICAL_MAP = {k: v for v, k in enumerate(RAG_LEVELS)}


def process_dq_data(s3_path, policy_and_roll_up_fp):

    policy_and_roll_up_dict = process_policy_and_roll_up(policy_and_roll_up_fp)

    df = awswrangler.s3.read_csv(s3_path, dataset=True)
    df = df.groupby(["extract_dt", "rule_id"]).last().reset_index()
    df['extract_dt'] = pandas.to_datetime(df['extract_dt'], format='%Y%m%d')
    rule_ids_in_current_policy = set(policy_and_roll_up_dict)
    df = df[df['rule_id'].isin(rule_ids_in_current_policy)]

    traffic_light_all_weeks_df = pandas.concat(
        [
            roll_up_rows(df[df['extract_dt'] == extract_dt], policy_and_roll_up_dict)
            for extract_dt in sorted(set(df['extract_dt']))
        ]
    )

    traffic_light_all_weeks_df['extract_dt'] = pandas.to_datetime(
        traffic_light_all_weeks_df['extract_dt'],
        format='%Y%m%d'
    )

    traffic_light_long_df = pandas.melt(
        traffic_light_all_weeks_df,
        id_vars=['extract_dt', 'rule_id']
    )

    traffic_light_summary_df = traffic_light_long_df.groupby(
        ['extract_dt', 'variable']
    )['value'].sum().reset_index()

    traffic_light_summary_df['color'] = traffic_light_summary_df['variable'].str.split('_').str[0].map(RAG_COLOR_MAP)
    
    overall_rag_data = []

    for dt in sorted(set(traffic_light_summary_df['extract_dt'])):

        df_x = traffic_light_summary_df[traffic_light_summary_df['extract_dt'] == dt]
        
        df_x = df_x[df_x['value'] > 0]
        
        total_issues = df_x[df_x['variable'].isin(RED_OR_AMBER)]['value'].sum()
        
        rag_level = sorted(set(df_x['variable']), key=lambda x: RAG_LEVEL_NUMERICAL_MAP.get(x, 0))[0]
        rag_level_numerical = RAG_LEVEL_NUMERICAL_MAP.get(rag_level, 0)
        rag_color = RAG_COLOR_MAP.get(rag_level.split('_')[0], wonderful_silver)
        overall_rag_data.append([dt, rag_level, rag_level_numerical, total_issues, rag_color])

    latest_rag_data = overall_rag_data[-1]

    latest_dt = max(set(traffic_light_summary_df['extract_dt']))
    previous_dt = max(set(traffic_light_summary_df['extract_dt']) - {latest_dt})

    latest_issues_rule_ids = set(traffic_light_long_df[
        (traffic_light_long_df['value'] > 0) &
        (traffic_light_long_df['variable'].isin(RED_OR_AMBER)) &
        (traffic_light_long_df['extract_dt'] == latest_dt)
    ]['rule_id'])

    previous_issues_rule_ids = set(traffic_light_long_df[
        (traffic_light_long_df['value'] > 0) &
        (traffic_light_long_df['variable'].isin(RED_OR_AMBER)) &
        (traffic_light_long_df['extract_dt'] == previous_dt)
    ]['rule_id'])

    new_issues_rule_ids = latest_issues_rule_ids - previous_issues_rule_ids
    recurring_rule_ids = latest_issues_rule_ids & previous_issues_rule_ids
    retreating_issues_rule_ids = previous_issues_rule_ids - latest_issues_rule_ids

    extra_df = traffic_light_long_df[
        (traffic_light_long_df['value'] > 0)
    ][
        ['extract_dt', 'rule_id', 'variable']
    ].drop_duplicates()


    extra_df = extra_df
    extra_df.columns = ['extract_dt', 'rule_id', 'dq_level']

    new_issues_df = df[
        (df['extract_dt'] == latest_dt) & (df['rule_id'].isin(new_issues_rule_ids))
    ].merge(extra_df, how='left')[
        [
            "rule_id",
            "validation_dataset",
            "rule_type",
            "column",
            "data_type_name",
            "condition",
            "values",
            "operator",
            "threshold",
            "validation_metric_value",
            "dq_level"
        ]
    ]

    recurring_issues_df = df[
        (df['extract_dt'] == latest_dt) & (df['rule_id'].isin(recurring_rule_ids))
    ].merge(extra_df, how='left')[
        [
            "rule_id",
            "validation_dataset",
            "rule_type",
            "column",
            "data_type_name",
            "condition",
            "values",
            "operator",
            "threshold",
            "validation_metric_value",
            "dq_level"
        ]
    ]

    retreating_issues_df = df[
        (df['extract_dt'] == latest_dt) & (df['rule_id'].isin(retreating_issues_rule_ids))
    ].merge(extra_df, how='left')[
        [
            "rule_id",
            "validation_dataset",
            "rule_type",
            "column",
            "data_type_name",
            "condition",
            "values",
            "operator",
            "threshold",
            "validation_metric_value",
            "dq_level"
        ]
    ].merge(extra_df, how='left')

    return (
        df,
        latest_rag_data,
        overall_rag_data,
        traffic_light_summary_df,
        new_issues_df,
        recurring_issues_df
    )


def create_app_components(
    latest_rag_data,
    overall_rag_data,
    traffic_light_summary_df,
    df
):

    dg = daq.Gauge(
        color={
            "gradient": True,
            "ranges": {
                wonderful_silver: [0, 1],
                wonderful_orange: [1, 4],
                wonderful_bright_yellow: [4, 7],
                wonderful_lime_green: [7, 10]
           }
        },
        value=latest_rag_data[2],
        # label='',
        max=10,
        min=0,
        className="dq-gauge-obj"
    )

    overall_rag_fig = go.Figure(
        {
            "layout": {
                "title": {
                    "text": "Overall Data Quality - Weekly Time Series: TPtEia - Weekly Reformed Scholars"
                }
            }
        }
    )

    overall_rag_fig.update_layout(yaxis_range=[0, 10])

    overall_rag_fig.add_trace(
        go.Scatter(
            x=[item[0] for item in overall_rag_data],
            y=[item[2] for item in overall_rag_data],
            marker={
                "color": [item[4] for item in overall_rag_data],
                "size": 23

            },
            line={
                "color": wonderful_dark_grey
            }
        )
    )

    rag_histogram_fig = go.Figure(
        {
            "layout": {
                "title": {
                    "text": "Red/Amber/Green Breakdowns - Weekly Histogram: TPtEia - Weekly Reformed Scholars"
                }
            }
        }
    )

    for i, v in enumerate(RAG_LEVELS[1:]):
        rag_histogram_fig.add_trace(
            go.Bar(
                x=traffic_light_summary_df[traffic_light_summary_df['variable'] == v]['extract_dt'],
                y=traffic_light_summary_df[traffic_light_summary_df['variable'] == v]['value'],
                width=24 * 3600 * 1000 / 2,
                offsetgroup=i,
                marker={
                    "color": traffic_light_summary_df[traffic_light_summary_df['variable'] == v]['color'],

                },
                name=v
            ),

        )

    rag_histogram_fig.update_layout(bargroupgap=0.0)

    return dg, overall_rag_fig, rag_histogram_fig, df


def render_content_for_app(tab, df):
    rule_df = df[df['rule_id'] == tab]
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=rule_df['extract_dt'].values,
            y=rule_df['validation_metric_value'].values,
            marker={
                "color": rule_df['constraint_status'].map(FAIL_SUCCESS_COLOR_MAP).values,
                "size": 11
            },
            name="value"
        ),

    )
    fig.add_trace(
        go.Scatter(
            x=rule_df['extract_dt'].values,
            y=rule_df['threshold'].values,
            mode="lines",
            line={
                "color": wonderful_dark_grey,
                "dash": "dot"
            },
            name="threshold"
        ),

    )
    a = min(-0.1, rule_df['validation_metric_value'].min(), rule_df['threshold'].min())
    b = max(1.1, rule_df['validation_metric_value'].max(), rule_df['threshold'].max())
    h = b - a
    fig.update_layout(

        yaxis_range=[a - 0.05 * h, b + 0.05 * h]
    )
    return html.Div(
        [
            html.H3('Time Series for {}'.format(tab)),
            dcc.Graph(id='overall_rag_graph', figure=fig)
        ]
    )


POLICY_AND_ROLL_UP_FP = 'policy_and_roll_up.csv'
S3_PATH = 's3://wonderful-bucket/terrific-subpartition/the-project-that-ended-it-all/data/validation/reformed_scholars_weekly'

""""""
app = Dash(__name__)

@app.callback(
    Output('tabs-example-content-1', 'children'),
    Input('tabs-example-1', 'value')
)
def render_content(tab):
    df = app.df
    return render_content_for_app(tab, df)

@app.callback(
    Output('live-dq-heading', 'children'),
    Input('interval-component', 'n_intervals')
)
def serve_heading(n):
    #just testing the five-second interval with the above decorator, not currently in use
    return ['Data Quality Dashboard: The Project That Ended It All', ]


def serve_layout():
    (
        df,
        latest_rag_data,
        overall_rag_data,
        traffic_light_summary_df,
        new_issues_df,
        recurring_issues_df

    ) = process_dq_data(
        S3_PATH,
        POLICY_AND_ROLL_UP_FP
    )
    app.df = df
    dg, overall_rag_fig, rag_histogram_fig, df = create_app_components(
        latest_rag_data,
        overall_rag_data,
        traffic_light_summary_df,
        df
    )

    all_rule_ids = sorted(set(df['rule_id']))

    return html.Div(
        children=[
            html.H1(id='live-dq-heading'),#, className="dashboard-heading"),
            html.Div(
                children=[
                    html.H3(
                        children='Current Extract - Overall DQ Level - {}'.format(
                            latest_rag_data[0].strftime('%Y-%m-%d')
                        ),
                        className="dq-gauge-obj"
                    ),
                    dg,
                    html.H3(
                        children='Current Extract - Total DQ Issues - {}'.format(latest_rag_data[3]),
                        className="dq-gauge-obj"
                    )
                ],
            ),
            dcc.Graph(id='overall_rag_graph', figure=overall_rag_fig),
            dcc.Graph(id='rag_histogram_graph', figure=rag_histogram_fig),
            html.H2(children='New Issues in Current Extract'),
            dash_table.DataTable(
                new_issues_df.to_dict('records'),
                [{"name": i, "id": i} for i in new_issues_df.columns],
                sort_action="native",
                filter_action="native"
            ),
            html.H2(children='Recurring Issues in Current Extract'),
            dash_table.DataTable(
                recurring_issues_df.to_dict('records'),
                [{"name": i, "id": i} for i in recurring_issues_df.columns],
                sort_action="native",
                filter_action="native",

                style_table={
                    'overflow': 'scroll',
                }
            ),
            html.Div([
                dcc.Tabs(
                    id='tabs-example-1',
                    value='tab-1',
                    children=[
                        dcc.Tab(label=str(int(rule_id.split('-')[-1])), value=rule_id)
                        for rule_id in all_rule_ids
                    ],
                    style={
                        'overflow': 'scroll',
                    }
                ),
                html.Div(id='tabs-example-content-1')
            ]),
            dcc.Interval(
                id='interval-component',
                interval=1 * 5000,  # in milliseconds
                n_intervals=0
            )
        ]
    )

app.layout = serve_layout

if __name__ == '__main__':
    app.run_server(host='0.0.0.0',port=8082)
