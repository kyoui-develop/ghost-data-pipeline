import pandas as pd
import pendulum


def filter_members(members, lang):
    return members[members['labels'].apply(
        lambda labels: any(label.get('slug') == lang for label in labels)
    )]


def filter_newsletters(newsletters, lang):
    return newsletters[newsletters['email_segment'].str.contains(lang)].squeeze()


def preprocess_members(func):
    def wrapper(members):
        members = pd.DataFrame(members)
        start_time = pendulum.now('Asia/Seoul').subtract(days=7).start_of('day').add(hours=10)
        end_time = pendulum.now('Asia/Seoul').start_of('day').add(hours=10)
        return [
            func(filter_members(members, 'english'), start_time, end_time, 'en'), 
            func(filter_members(members, 'korean'), start_time, end_time, 'ko')
        ]
    return wrapper


def preprocess_newsletters(func):
    def wrapper(newsletters):
        newsletters = pd.DataFrame(newsletters)
        return [
            func(filter_newsletters(newsletters, 'english'), 'en'), 
            func(filter_newsletters(newsletters, 'korean'), 'ko')
        ] 
    return wrapper


@preprocess_members
def extract_member_statistics(members, start_time, end_time, lang_code):
    date = pendulum.now('Asia/Seoul').format('YYYY-MM-DD')
    total = len(members)
    active = len(members[
        (pd.to_datetime(members['last_seen_at']).dt.tz_convert('Asia/Seoul') >= start_time)
        & (pd.to_datetime(members['last_seen_at']).dt.tz_convert('Asia/Seoul') < end_time)
    ])
    subscriber = len(members[
        (members['subscribed'] == True)
        & (members['email_suppression'].apply(lambda x: x['suppressed'] == False))
    ])
    engagement_rate = round((active / total), 3)
    subscription_rate = round((subscriber / total), 3)
    return {
        'date': date,
        'total': total,
        'active': active,
        'subscriber': subscriber,
        'engagement_rate' : engagement_rate,
        'subscription_rate': subscription_rate,
        'lang_code': lang_code
    } 


@preprocess_members
def extract_subscriber_statistics(members, start_time, end_time, lang_code):
    date = pendulum.now('Asia/Seoul').format('YYYY-MM-DD')
    total = len(members[
        (members['subscribed'] == True)
        & (members['email_suppression'].apply(lambda x: x['suppressed'] == False))
    ])
    new = len(members[
        (pd.to_datetime(members['created_at']).dt.tz_convert('Asia/Seoul') >= start_time)
        & (pd.to_datetime(members['created_at']).dt.tz_convert('Asia/Seoul') < end_time)
        & (members['email_suppression'].apply(lambda x: x['suppressed'] == False))
    ])
    churn = len(members[
        (members['subscribed'] == False)
        & (pd.to_datetime(members['last_seen_at']).dt.tz_convert('Asia/Seoul') >= start_time)
        & (pd.to_datetime(members['last_seen_at']).dt.tz_convert('Asia/Seoul') < end_time)
        & (members['email_suppression'].apply(lambda x: x['suppressed'] == False))
    ])
    net = new - churn
    return {
        'date': date,
        'total': total,
        'new': new,
        'churn': churn,
        'net': net,
        'lang_code': lang_code
    }


@preprocess_newsletters
def extract_newsletter_statistics(newsletter, lang_code):
    date = pendulum.parse(newsletter['published_at']).in_timezone('Asia/Seoul').format('YYYY-MM-DD')
    sent = newsletter['email']['email_count']
    opened = newsletter['email']['opened_count']
    clicked = newsletter['count']['clicks']
    delivered = newsletter['email']['delivered_count']
    open_rate = round((opened / delivered), 3)
    click_rate = round((clicked / delivered), 3)
    delivery_rate = round((delivered / sent), 3)
    return {
        'date': date,
        'sent': sent,
        'opened': opened,
        'clicked': clicked,
        'delivered': delivered,
        'open_rate': open_rate,
        'click_rate': click_rate,
        'delivery_rate': delivery_rate,
        'title': newsletter['title'],
        'lang_code': lang_code
    }


def preprocess(data):
    return {
        'members': data['members'],
        'newsletters': data['newsletters'],
        'member_stats': extract_member_statistics(data['members']),
        'subscriber_stats': extract_subscriber_statistics(data['members']),
        'newsletter_stats': extract_newsletter_statistics(data['newsletters'])
    }