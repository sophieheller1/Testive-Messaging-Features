"""
"""
import json
import datetime

from django.core.urlresolvers import reverse

from hbs.test_utils import TestCase
from hbs import generic_utils as utils
from hbs import date_utils
from coaching import models as coaching
from testsession.models import ItinerarySessionQuestion
from testsession.tests.factories import ItineraryFactory
from questions.tests.factories import QuestionFactory
from scheduling.models import StudyDay
from coaching.tasks import UpdateCoachNextActions, MissedWorkActions, MissedTwoDaysInRowAction, UploadedPracticeTestAction
from meetings.models import Meeting, Service


class TestMissedTwoDaysTask(TestCase):
    
    def setUp(self):
        self.coach = self.create_user('antonio@hardy.com')
        self.student = self.create_user('shawn@carter.com')
        self.testive = coaching.Organization.objects.create(name='Testive')
        self.algebra_constraints = [
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},

        ]
        self.sat_algebra_questions = QuestionFactory.create_batch(
            30, qualities=('Algebra', 'SAT'), license_owner='testive')
        self.itinerary = ItineraryFactory.create(_constraints=self.algebra_constraints)

        coaching.CoachStudent.objects.create(coach=self.coach, student=self.student)
        coaching.CoachOrganization.objects.create(coach=self.coach, organization=self.testive)

    def _create_study_days(self, num_days, user=None, offset=0):
        user = user if user else self.student
        for i in range(num_days):
            date = date_utils.utc_now() - datetime.timedelta(days=(i + offset))
            StudyDay.objects.create(
                user=user, date=date, question_commitment=10, day_type=StudyDay.STUDY)

    def _do_work(self, num_days, user=None, offset=0):
        user = user if user else self.student
        for i in range(num_days):
            date = date_utils.utc_now() - datetime.timedelta(days=(i + offset))
            path, isession = self.step_through_itinerary(user, self.itinerary, answer_limit=10)
            for question in path:
                isq = ItinerarySessionQuestion.objects.get(itinerary_session=isession, question=question)
                isq.created_on = date
                isq.save()

    def test_two_missed_days_creates_correct_action(self): 
        
        self._create_study_days(2, offset=1)

        MissedTwoDaysInRowAction()._create(self.coach, self.student)

        next_actions = coaching.NextActions.objects.filter(
            student=self.student, coach=self.coach, action_type=coaching.NextActions.MISSED_TWO_DAYS_IN_ROW)

        self.assertEqual(next_actions.count(), 1)
    
    def test_no_missed_days_does_not_create_action(self): 

        self._create_study_days(2, offset=1)
        self._do_work(2)

        MissedTwoDaysInRowAction()._create(self.coach, self.student)

        next_actions = coaching.NextActions.objects.filter(
            student=self.student, coach=self.coach, action_type=coaching.NextActions.MISSED_TWO_DAYS_IN_ROW)

        self.assertEqual(next_actions.count(), 0)

    def test_no_study_days_does_not_create_action_and_does_not_error(self): 

        MissedTwoDaysInRowAction()._create(self.coach, self.student)

        next_actions = coaching.NextActions.objects.filter(
            student=self.student, coach=self.coach, action_type=coaching.NextActions.MISSED_TWO_DAYS_IN_ROW)

        self.assertEqual(next_actions.count(), 0)

    def test_today_is_ignored_for_missed_sessions(self): 
    
        self._create_study_days(3)

        MissedTwoDaysInRowAction()._create(self.coach, self.student)

        next_actions = coaching.NextActions.objects.filter(
            student=self.student, coach=self.coach, action_type=coaching.NextActions.MISSED_TWO_DAYS_IN_ROW)

        self.assertEqual(next_actions.count(), 1)

    def test_only_missed_complete_day_does_not_create_action(self): 

        self._create_study_days(2, offset=1)
        self._do_work(1, offset=1)

        MissedTwoDaysInRowAction()._create(self.coach, self.student)

        next_actions = coaching.NextActions.objects.filter(student=self.student, coach=self.coach, action_type=coaching.NextActions.MISSED_TWO_DAYS_IN_ROW)

        self.assertEqual(next_actions.count(), 0)

    def test_only_completed_missed_day_does_not_create_action(self): 
        
        self._create_study_days(2, offset=1)
        self._do_work(1, offset=2)

        MissedTwoDaysInRowAction()._create(self.coach, self.student)

        next_actions = coaching.NextActions.objects.filter(student=self.student, coach=self.coach, action_type=coaching.NextActions.MISSED_TWO_DAYS_IN_ROW)

        self.assertEqual(next_actions.count(), 0) 

    def test_two_missed_days_in_past_do_not_create_action(self): 
        self._create_study_days(10, offset=1)
        self._do_work(2)

        MissedTwoDaysInRowAction()._create(self.coach, self.student)

        next_actions = coaching.NextActions.objects.filter(
            student=self.student, coach=self.coach, action_type=coaching.NextActions.MISSED_TWO_DAYS_IN_ROW)

        self.assertEqual(next_actions.count(), 0)


class TestUploadedPracticeTestTask(TestCase): 
    
    def setUp(self):
        self.coach = self.create_user('antonio@hardy.com')
        self.student = self.create_user('shawn@carter.com')
        self.testive = coaching.Organization.objects.create(name='Testive')
        self.algebra_constraints = [
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},
            {'test': ['SAT'], 'qualities': ['Algebra'], 'license_owner': ['testive']},

        ]
        self.sat_algebra_questions = QuestionFactory.create_batch(
            30, qualities=('Algebra', 'SAT'), license_owner='testive')
        self.itinerary = ItineraryFactory.create(is_practice_test=True, _constraints=self.algebra_constraints)

        coaching.CoachStudent.objects.create(coach=self.coach, student=self.student)
        coaching.CoachOrganization.objects.create(coach=self.coach, organization=self.testive)

    def _create_study_days(self, num_days, user=None, offset=0, day_type=StudyDay.STUDY):
        user = user if user else self.student
        for i in range(num_days):
            date = date_utils.utc_now() - datetime.timedelta(days=(i + offset))
            StudyDay.objects.create(
                user=user, date=date, question_commitment=10, day_type=day_type)

    def _do_work(self, num_days, user=None, offset=0):
        user = user if user else self.student
        for i in range(num_days):
            date = date_utils.utc_now() - datetime.timedelta(days=(i + offset))
            path, isession = self.step_through_itinerary(user, self.itinerary, answer_limit=10)
            for question in path:
                isq = ItinerarySessionQuestion.objects.get(itinerary_session=isession, question=question)
                isq.created_on = date
                isq.save()


    def test_one_practice_test_uploaded_creates_action(self):
      
        self._create_study_days(1)
        self._do_work(1)   

        UploadedPracticeTestAction()._create(self.coach, self.student)

        next_actions = coaching.NextActions.objects.filter(
            student=self.student, coach=self.coach, action_type=coaching.NextActions.UPLOADED_PRACTICE_TEST)

        self.assertEqual(next_actions.count(), 1)


    def test_different_day_type_creates_action(self):  
        
        self._create_study_days(1, day_type=StudyDay.BREAK)
        self._do_work(1)

        UploadedPracticeTestAction()._create(self.coach, self.student)

        next_actions = coaching.NextActions.objects.filter(
            student=self.student, coach=self.coach, action_type=coaching.NextActions.UPLOADED_PRACTICE_TEST)

        self.assertEqual(next_actions.count(), 1)

    
    def test_no_practice_test_uploaded_does_not_create_action(self):   
        
        self._create_study_days(1)

        UploadedPracticeTestAction()._create(self.coach, self.student)

        next_actions = coaching.NextActions.objects.filter(
            student=self.student, coach=self.coach, action_type=coaching.NextActions.UPLOADED_PRACTICE_TEST)

        self.assertEqual(next_actions.count(), 0)


    def test_one_practice_test_over_multiple_study_days_creates_action(self):

        self._create_study_days(7)
        self._do_work(1, offset=4)

        UploadedPracticeTestAction()._create(self.coach, self.student)

        next_actions = coaching.NextActions.objects.filter(
            student=self.student, coach=self.coach, action_type=coaching.NextActions.UPLOADED_PRACTICE_TEST)

        self.assertEqual(next_actions.count(), 1)
